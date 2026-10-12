#!/usr/bin/env bash
source $(dirname $0)/../scripts/utils.sh
source $(dirname $0)/../scripts/mlx_env.sh
export HRD_REGISTRY_IP="192.168.1.9"

drop_shm

# lsync messes up permissions
executable="../build/sender-scalability"
chmod +x $executable

blue "Running client thread"

# Check number of arguments
if [ "$#" -lt 1 ] || [ "$#" -gt 4 ]; then
  blue "Illegal number of arguments."
  blue "Usage: ./run-machine.sh <machine_id> [xrc] [xrc_qps=N] [gdb]"
  exit 1
fi

use_xrc=0
use_gdb=0
xrc_qps_per_thread=1
for arg in "${@:2}"; do
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
      blue "Usage: ./run-machine.sh <machine_id> [xrc] [xrc_qps=N] [gdb]"
      exit 1
      ;;
  esac
done

flags="\
	--dual_port 0 \
	--use_uc 0 \
  --use_xrc $use_xrc \
  --xrc_qps_per_thread $xrc_qps_per_thread \
  --run_time 1000 \
	--is_client 1 \
	--machine_id $1
"

# Check for non-gdb mode
if [ "$use_gdb" -eq 0 ]; then
  # sudo -E numactl --cpunodebind=0 --membind=0 $executable $flags
  sudo -E numactl --cpunodebind=1 --membind=1 ../build/sender-scalability $flags
fi

# Check for gdb mode
if [ "$use_gdb" -eq 1 ]; then
  sudo -E gdb -ex run --args $executable $flags
fi
