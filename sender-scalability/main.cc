#include <gflags/gflags.h>
#include <limits>
#include <pthread.h>
#include <thread>
#include <vector>
#include "libhrd_cpp/hrd.h"

static constexpr size_t kAppBufSize = MB(2);
static constexpr int kAppBaseSHMKey = 2;

static constexpr size_t kAppDefaultRunTime = 1000000;
static constexpr size_t kAppRunTimeSlack = 10;

// Number of outstanding requests kept by a server thread across all QPs.
// This is only used for READs where we can detect completion by polling on
// a READ's destination buffer.
//
// For WRITEs, this is hard to do unless we make every send() signaled. So,
// the number of per-thread outstanding operations per thread with WRITEs is
// O(NUM_CLIENTS * kAppUnsigBatch).
static constexpr size_t kAppWindowSize = 32;
static_assert(is_power_of_two(kAppWindowSize), "");

// Sweep paramaters
static constexpr size_t kAppNumServers = 32;
static constexpr size_t kAppNumClients = 512;  // Total client QPs in cluster
static constexpr size_t kAppNumClientMachines = 1;
static constexpr size_t kAppUnsigBatch = 1;

static_assert(kHrdSQDepth == 128, "");  // Small queues => more scalaing
static_assert(kAppNumClients % kAppNumClientMachines == 0, "");

// We don't use postlist, so we don't need a postlist check
static_assert(kHrdSQDepth >= 2 * kAppUnsigBatch, "");  // Queue capacity check

struct thread_params_t {
  size_t id;
  double* tput;
  pthread_barrier_t* start_barrier;
};

DEFINE_uint64(machine_id, std::numeric_limits<size_t>::max(), "Machine ID");
DEFINE_uint64(is_client, 0, "Is this process a client?");
DEFINE_uint64(run_time, 0, "Running time");
DEFINE_uint64(dual_port, 0, "Use two ports?");
DEFINE_uint64(use_uc, 0, "Use unreliable connected transport?");
DEFINE_uint64(use_xrc, 0, "Use XRC transport instead of RC?");
DEFINE_uint64(xrc_qps_per_thread, 1,
              "XRC_SEND QPs per server thread (XRC mode only)");
DEFINE_uint64(do_read, 0, "Do RDMA reads?");
DEFINE_uint64(size, 0, "RDMA size");

void run_server(thread_params_t* params) {
  size_t srv_gid = params->id;  // Global ID of this server thread
  size_t ib_port_index = FLAGS_dual_port == 0 ? 0 : srv_gid % 2;
  int shm_key = kAppBaseSHMKey + static_cast<int>(srv_gid);

  hrd_conn_config_t conn_config;
  conn_config.num_qps = FLAGS_use_xrc == 1 ? FLAGS_xrc_qps_per_thread
                                            : kAppNumClients;
  conn_config.use_uc = (FLAGS_use_uc == 1);
  conn_config.use_xrc = (FLAGS_use_xrc == 1);
  conn_config.xrc_create_send_qp = true;
  conn_config.xrc_create_recv_qp = false;
  conn_config.prealloc_buf = nullptr;
  conn_config.buf_size = kAppBufSize;
  conn_config.buf_shm_key = shm_key;

  auto* cb =
      hrd_ctrl_blk_init(srv_gid, ib_port_index, 1, &conn_config, nullptr);

  // Set the buffer to 0 so that we can detect READ completion by polling.
  memset(const_cast<uint8_t*>(cb->conn_buf), 0, kAppBufSize);

  std::vector<hrd_qp_attr_t*> clt_qp(kAppNumClients, nullptr);
  std::vector<hrd_qp_attr_t*> xrc_target(conn_config.num_qps, nullptr);
  if (FLAGS_use_xrc == 1) {
    // Publish every source QP first. The receiver creates one XRC_RECV QP
    // per source QP and transitions it to RTR/RTS against this endpoint.
    for (size_t qp = 0; qp < conn_config.num_qps; qp++) {
      char send_name[kHrdQPNameSize];
      sprintf(send_name, "xrc-send-server-%zu-%zu", srv_gid, qp);
      hrd_publish_xrc_send_qp(cb, qp, send_name);
    }

    for (size_t qp = 0; qp < conn_config.num_qps; qp++) {
      char target_name[kHrdQPNameSize];
      const size_t qps_per_machine =
          FLAGS_xrc_qps_per_thread / kAppNumClientMachines;
      sprintf(target_name, "xrc-target-machine-%zu-server-%zu-%zu",
              qp / qps_per_machine, srv_gid, qp);
      while (xrc_target[qp] == nullptr) {
        xrc_target[qp] = hrd_get_published_qp(target_name);
        if (xrc_target[qp] == nullptr) usleep(20000);
      }
      hrd_connect_qp(cb, qp, xrc_target[qp]);
    }

    for (size_t i = 0; i < kAppNumClients; i++) {
      char buf_name[kHrdQPNameSize];
      sprintf(buf_name, "client-buf-%zu", i);
      while (clt_qp[i] == nullptr) {
        clt_qp[i] = hrd_get_published_qp(buf_name);
        if (clt_qp[i] == nullptr) usleep(20000);
      }
    }
  } else {
    // Keep the original RC topology: one QP for every remote client thread.
    for (size_t i = 0; i < kAppNumClients; i++) {
      char srv_qp_name[kHrdQPNameSize];
      sprintf(srv_qp_name, "server-%zu-%zu", srv_gid, i);
      hrd_publish_conn_qp(cb, i, srv_qp_name);
    }
    for (size_t i = 0; i < kAppNumClients; i++) {
      char clt_qp_name[kHrdQPNameSize];
      sprintf(clt_qp_name, "client-%zu-%zu", i, srv_gid);
      while (clt_qp[i] == nullptr) {
        clt_qp[i] = hrd_get_published_qp(clt_qp_name);
        if (clt_qp[i] == nullptr) usleep(20000);
      }
      printf("main: Server %zu found client %zu! Connecting..\n", srv_gid, i);
      hrd_connect_qp(cb, i, clt_qp[i]);
      hrd_wait_till_ready(clt_qp_name);
    }
  }

  printf("main: Server %zu ready\n", srv_gid);

  // Do not include unequal connection/setup time in the measurement window.
  const int barrier_ret = pthread_barrier_wait(params->start_barrier);
  rt_assert(barrier_ret == 0 || barrier_ret == PTHREAD_BARRIER_SERIAL_THREAD,
            "Server start barrier failed");

  struct ibv_send_wr wr, *bad_send_wr;
  struct ibv_sge sgl;
  struct ibv_wc wc;
  size_t rolling_iter = 0;             // For performance measurement
  std::vector<size_t> nb_tx(conn_config.num_qps, 0);  // Per-QP signaling
  size_t nb_tx_tot = 0;                // For windowing (for READs only)

  struct timespec run_start, run_end;
  struct timespec msr_start, msr_end;
  clock_gettime(CLOCK_REALTIME, &run_start);
  clock_gettime(CLOCK_REALTIME, &msr_start);

  auto opcode = FLAGS_do_read == 0 ? IBV_WR_RDMA_WRITE : IBV_WR_RDMA_READ;
  uint64_t seed = 0xdeadbeef;

  while (1) {
    if (rolling_iter >= MB(1)) {
      clock_gettime(CLOCK_REALTIME, &msr_end);
      double msr_seconds = (msr_end.tv_sec - msr_start.tv_sec) +
                           (msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000.0;
      double tput = rolling_iter / msr_seconds;
      double gbps = tput * FLAGS_size * 8.0 / 1e9;

      clock_gettime(CLOCK_REALTIME, &run_end);
      double run_seconds = (run_end.tv_sec - run_start.tv_sec) +
                           (run_end.tv_nsec - run_start.tv_nsec) / 1000000000.0;
      if (run_seconds >= FLAGS_run_time) {
        printf("main: Server %zu exiting.\n", srv_gid);
        hrd_ctrl_blk_destroy(cb);
        return;
      }

      printf(
          "main: Server %zu: %.2f Gbps (%.2f ops/s). Total active SEND QPs = %zu. "
          "Outstanding ops per thread (for READs) = %zu. "
          "Seconds = %.1f of %zu.\n",
          srv_gid, gbps, tput,
          kAppNumServers * (FLAGS_use_xrc == 1 ? FLAGS_xrc_qps_per_thread
                                                : kAppNumClients),
          kAppWindowSize,
          run_seconds, FLAGS_run_time);

      params->tput[srv_gid] = tput;
      if (srv_gid == 0) {
        double tot = 0;
        for (size_t i = 0; i < kAppNumServers; i++) tot += params->tput[i];
        hrd_red_printf("Total tput = %.2f Gbps (%.2f ops/s)\n",
                       tot * FLAGS_size * 8.0 / 1e9, tot);
      }

      rolling_iter = 0;
      clock_gettime(CLOCK_REALTIME, &msr_start);
    }

    size_t window_i = nb_tx_tot % kAppWindowSize;  // Current window slot to use

    // For READs, restrict outstanding ops per-thread to kAppWindowSize
    if (opcode == IBV_WR_RDMA_READ && nb_tx_tot >= kAppWindowSize) {
      while (cb->conn_buf[window_i * FLAGS_size] == 0) {
        // Wait for a window slow to open up
      }
      cb->conn_buf[window_i * FLAGS_size] = 0;
    }

    // Choose the next client to send a packet to
    size_t cn = hrd_fastrand(&seed) % kAppNumClients;
    wr.opcode = opcode;
    wr.num_sge = 1;
    wr.next = nullptr;
    wr.sg_list = &sgl;

    size_t qp_i = cn;
    if (FLAGS_use_xrc == 1) {
      constexpr size_t clients_per_machine =
          kAppNumClients / kAppNumClientMachines;
      const size_t machine = cn / clients_per_machine;
      const size_t qps_per_machine =
          FLAGS_xrc_qps_per_thread / kAppNumClientMachines;
      qp_i = machine * qps_per_machine +
             (cn % clients_per_machine) % qps_per_machine;
    }
    wr.send_flags = nb_tx[qp_i] % kAppUnsigBatch == 0 ? IBV_SEND_SIGNALED : 0;
    if (nb_tx[qp_i] % kAppUnsigBatch == 0 && nb_tx[qp_i] > 0) {
      // This can happen if a client dies before the server
      int ret = hrd_poll_cq_ret(cb->conn_cq[qp_i], 1, &wc);
      if (ret == -1) {
        hrd_ctrl_blk_destroy(cb);
        return;
      }
    }

    // wr.send_flags |= (FLAGS_do_read == 0) ? IBV_SEND_INLINE : 0;

    sgl.addr = reinterpret_cast<uint64_t>(&cb->conn_buf[window_i * FLAGS_size]);
    sgl.length = FLAGS_size;
    sgl.lkey = cb->conn_buf_mr->lkey;
    // //随机偏移地址
    // size_t remote_offset = hrd_fastrand(&seed) % (kAppBufSize - FLAGS_size);
    //固定地址
    size_t remote_offset = 0;
    wr.wr.rdma.remote_addr = clt_qp[cn]->buf_addr + remote_offset;
    wr.wr.rdma.rkey = clt_qp[cn]->rkey;
    if (FLAGS_use_xrc == 1) {
      wr.qp_type.xrc.remote_srqn = xrc_target[qp_i]->xrc_srqn;
    }

    nb_tx[qp_i]++;

    int ret = ibv_post_send(cb->conn_qp[qp_i], &wr, &bad_send_wr);
    rt_assert(ret == 0);
    rolling_iter++;
  }
}

void run_client(thread_params_t* params) {
  constexpr size_t num_threads = kAppNumClients / kAppNumClientMachines;

  size_t clt_gid = params->id;  // Global ID of this server thread
  size_t ib_port_index = FLAGS_dual_port == 0 ? 0 : clt_gid % 2;
  int shm_key = kAppBaseSHMKey + clt_gid % num_threads;
  const bool xrc_target_owner = FLAGS_use_xrc == 1 && clt_gid % num_threads == 0;

  hrd_conn_config_t conn_config;
  conn_config.num_qps = FLAGS_use_xrc == 1
                            ? (xrc_target_owner
                                   ? kAppNumServers *
                                         (FLAGS_xrc_qps_per_thread /
                                          kAppNumClientMachines)
                                   : 0)
                            : kAppNumServers;
  conn_config.use_uc = (FLAGS_use_uc == 1);
  conn_config.use_xrc = (FLAGS_use_xrc == 1);
  conn_config.xrc_create_send_qp = FLAGS_use_xrc == 0;
  conn_config.xrc_create_recv_qp = xrc_target_owner;
  conn_config.prealloc_buf = nullptr;
  conn_config.buf_size = kAppBufSize;
  conn_config.buf_shm_key = shm_key;

  auto* cb =
      hrd_ctrl_blk_init(clt_gid, ib_port_index, 1, &conn_config, nullptr);

  // Set to some non-zero value so the server can detect READ completion
  memset(const_cast<uint8_t*>(cb->conn_buf), 1, kAppBufSize);

  if (FLAGS_use_xrc == 1) {
    char buf_name[kHrdQPNameSize];
    sprintf(buf_name, "client-buf-%zu", clt_gid);
    hrd_publish_conn_buf(cb, buf_name);

    if (xrc_target_owner) {
      // An XRC target is connection-specific. There are only
      // kAppNumServers * xrc_qps_per_thread of them, independent of the
      // number of receiver workers/buffers.
      const size_t qps_per_machine =
          FLAGS_xrc_qps_per_thread / kAppNumClientMachines;
      for (size_t server = 0; server < kAppNumServers; server++) {
        for (size_t qp = 0; qp < qps_per_machine; qp++) {
          const size_t send_qp_i = FLAGS_machine_id * qps_per_machine + qp;
          const size_t recv_qp_i = server * qps_per_machine + qp;
          char send_name[kHrdQPNameSize];
          sprintf(send_name, "xrc-send-server-%zu-%zu", server, send_qp_i);
          hrd_qp_attr_t* remote_send = nullptr;
          while (remote_send == nullptr) {
            remote_send = hrd_get_published_qp(send_name);
            if (remote_send == nullptr) usleep(20000);
          }
          hrd_connect_xrc_recv_qp(cb, recv_qp_i, remote_send);

          char target_name[kHrdQPNameSize];
          sprintf(target_name, "xrc-target-machine-%zu-server-%zu-%zu",
                  FLAGS_machine_id, server, send_qp_i);
          hrd_publish_conn_qp(cb, recv_qp_i, target_name);
        }
      }
    }
  } else {
    // Keep the original RC topology unchanged.
    for (size_t i = 0; i < kAppNumServers; i++) {
      char clt_qp_name[kHrdQPNameSize];
      sprintf(clt_qp_name, "client-%zu-%zu", clt_gid, i);
      hrd_publish_conn_qp(cb, i, clt_qp_name);
    }

    for (size_t i = 0; i < kAppNumServers; i++) {
      char srv_qp_name[kHrdQPNameSize];
      sprintf(srv_qp_name, "server-%zu-%zu", i, clt_gid);

      hrd_qp_attr_t* srv_qp = nullptr;
      while (srv_qp == nullptr) {
        srv_qp = hrd_get_published_qp(srv_qp_name);
        if (srv_qp == nullptr) usleep(20000);
      }

      printf("main: Client %zu found server %zu! Connecting..\n", clt_gid, i);
      hrd_connect_qp(cb, i, srv_qp);

      char clt_qp_name[kHrdQPNameSize];
      sprintf(clt_qp_name, "client-%zu-%zu", clt_gid, i);
      hrd_publish_ready(clt_qp_name);
    }
  }

  printf("main: Client %zu READY\n", clt_gid);

  const int barrier_ret = pthread_barrier_wait(params->start_barrier);
  rt_assert(barrier_ret == 0 || barrier_ret == PTHREAD_BARRIER_SERIAL_THREAD,
            "Client start barrier failed");

  struct timespec run_start, run_end;
  clock_gettime(CLOCK_REALTIME, &run_start);

  while (true) {
    printf("main: Client %zu: %d\n", clt_gid, cb->conn_buf[0]);

    clock_gettime(CLOCK_REALTIME, &run_end);
    double run_seconds = (run_end.tv_sec - run_start.tv_sec) +
                         (run_end.tv_nsec - run_start.tv_nsec) / 1000000000.0;

    if (run_seconds >= FLAGS_run_time + kAppRunTimeSlack) {
      printf("main: Client %zu: exiting\n", clt_gid);
      hrd_ctrl_blk_destroy(cb);
      return;
    } else {
      printf("main: Client %zu: active for %.2f seconds (of %zu + %zu)\n",
             clt_gid, run_seconds, FLAGS_run_time, kAppRunTimeSlack);
    }

    sleep(1);
  }
}

int main(int argc, char* argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  rt_assert(FLAGS_dual_port <= 1, "Invalid dual_port");
  rt_assert(FLAGS_use_uc <= 1, "Invalid use_uc");
  rt_assert(FLAGS_use_xrc <= 1, "Invalid use_xrc");
  rt_assert(FLAGS_is_client <= 1, "Invalid is_client");
  rt_assert(!(FLAGS_use_xrc == 1 && FLAGS_use_uc == 1),
            "XRC and UC cannot be used together");
  if (FLAGS_use_xrc == 1) {
    rt_assert(FLAGS_xrc_qps_per_thread >= kAppNumClientMachines,
              "Need at least one XRC_SEND QP per remote machine");
    rt_assert(FLAGS_xrc_qps_per_thread % kAppNumClientMachines == 0,
              "xrc_qps_per_thread must divide evenly across remote machines");
  }

  size_t num_threads;
  if (FLAGS_is_client == 1) {
    rt_assert(FLAGS_machine_id < kAppNumClientMachines, "Invalid machine ID");
    num_threads = kAppNumClients / kAppNumClientMachines;
  } else {
    // All the buffers sent or received should fit in @conn_buf
    rt_assert(FLAGS_size * kAppWindowSize < kAppBufSize, "");

    num_threads = kAppNumServers;
    rt_assert(FLAGS_machine_id == std::numeric_limits<size_t>::max(), "");
    rt_assert(FLAGS_size > 0, "Invalid size");

    // if (FLAGS_do_read == 0) rt_assert(FLAGS_size <= kHrdMaxInline, "Inl error");
    rt_assert(FLAGS_do_read <= 1, "Invalid do_read");
  }

  // Launch the server or client threads
  printf("main: Launching %zu threads\n", num_threads);
  std::vector<thread_params_t> param_arr(num_threads);
  std::vector<std::thread> thread_arr(num_threads);
  std::vector<double> tput(num_threads, 0.0);
  pthread_barrier_t start_barrier;
  rt_assert(pthread_barrier_init(&start_barrier, nullptr, num_threads) == 0,
            "Failed to create thread start barrier");
  for (size_t i = 0; i < num_threads; i++) {
    if (FLAGS_is_client == 1) {
      param_arr[i].id = (FLAGS_machine_id * num_threads) + i;
      param_arr[i].start_barrier = &start_barrier;
      thread_arr[i] = std::thread(run_client, &param_arr[i]);
    } else {
      param_arr[i].id = i;
      param_arr[i].tput = tput.data();
      param_arr[i].start_barrier = &start_barrier;
      thread_arr[i] = std::thread(run_server, &param_arr[i]);
    }
  }

  for (auto& t : thread_arr) t.join();
  rt_assert(pthread_barrier_destroy(&start_barrier) == 0,
            "Failed to destroy thread start barrier");
  return 0;
}
