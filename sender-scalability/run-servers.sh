#!/usr/bin/env bash
source $(dirname $0)/../scripts/utils.sh
source $(dirname $0)/../scripts/mlx_env.sh
export HRD_REGISTRY_IP="192.168.1.9"

# # Always rebuild before changing the registry or starting the benchmark. This
# # makes each invocation run the binary corresponding to the current sources.
# blue "Building sender-scalability"
# if ! sudo -E make -C "$(dirname "$0")/../build" --always-make sender-scalability; then
#   blue "Build failed; server was not started."
#   exit 1
# fi

drop_shm

blue "Reset server QP registry"
sudo pkill memcached
memcached -l 0.0.0.0 1>/dev/null 2>/dev/null &
sleep 1

blue "Starting server"

use_xrc=0
use_gdb=0
xrc_qps_per_thread=1
for arg in "$@"; do
  case "$arg" in
    xrc) use_xrc=1 ;;
    gdb) use_gdb=1 ;;
    xrc_qps=*)
      xrc_qps_per_thread="${arg#xrc_qps=}"
      [[ "$xrc_qps_per_thread" =~ ^[1-9][0-9]*$ ]] || {
        blue "xrc_qps must be a positive integer"
        exit 1
      }
      ;;
    *)
      blue "Usage: ./run-servers.sh [xrc] [xrc_qps=N] [gdb]"
      exit 1
      ;;
  esac
done

flags="
	--dual_port 0 \
  --use_uc 0 \
  --use_xrc $use_xrc \
  --xrc_qps_per_thread $xrc_qps_per_thread \
	--is_client 0 \
	--size 1024 \
	--run_time 1000 \
	--do_read 0
"

# Check for non-gdb mode
if [ "$use_gdb" -eq 0 ]; then
  # sudo -E numactl --cpunodebind=0 --membind=0 ../build/sender-scalability $flags
  sudo -E numactl --cpunodebind=1 --membind=1 ../build/sender-scalability $flags
fi

# Check for gdb mode
if [ "$use_gdb" -eq 1 ]; then
  sudo -E gdb -ex run --args ../build/sender-scalability $flags
fi
