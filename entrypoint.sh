#!/bin/sh
set -eu

# On ERR signal, write to tty and exit
trap 'echo "ERROR at line $LINENO" >&2; exit 1' ERR

echo "LOADING ENV VARS"

#=========================
# CPU
#=========================
export SYS_CPU_COUNT="$(nproc)"
#SINGLE CORE MACHINE WONT RUN
export PROC_CPU_COUNT="$(( SYS_CPU_COUNT > 1 ? SYS_CPU_COUNT / 2 : 2 ))"

echo "NUMBER OF CORES:"
echo "SYS_CPU_COUNT=${SYS_CPU_COUNT}"
echo "PROC_CPU_COUNT=${PROC_CPU_COUNT}"

#=========================
# Memory
#=========================
export SYS_MEMORY_TOTAL="$(
  python3 - <<'EOF'
import psutil
print(psutil.virtual_memory().total)
EOF
)"


export PROC_MEMORY_BUDGET="$SYS_MEMORY_TOTAL"

echo "MEMORY BUDGET:"
echo "SYS_MEMORY_TOTAL=${SYS_MEMORY_TOTAL}"
echo "PROC_MEMORY_BUDGET=${PROC_MEMORY_BUDGET}"

# ----------------------
# Cache sizes (testing only)
# ----------------------
# Defaults
SYS_L1_CACHE_SIZE=32768       # 32 KB
SYS_L2_CACHE_SIZE=524288      # 512 KB
SYS_L3_CACHE_SIZE=8388608     # 8 MB
CACHE_SRC="fallback-defaults"

parse_size() {
  case "$1" in
    *K) echo $(( ${1%K} * 1024 )) ;;
    *M) echo $(( ${1%M} * 1024 * 1024 )) ;;
    *)  echo "$1" ;;
  esac
}

#SOME LINUX DISTRIBUTION MIGHT HAVE DIFF PATH OR MIGHT NOT EVEN HAVE THIS
#ADJUST ACCORDINGLY
if L1_RAW="$(cat /sys/devices/system/cpu/cpu0/cache/index0/size 2>/dev/null)"; then
  SYS_L1_CACHE_SIZE="$(parse_size "$L1_RAW")"
  SYS_L2_CACHE_SIZE="$(parse_size "$(cat /sys/devices/system/cpu/cpu0/cache/index2/size)")"
  SYS_L3_CACHE_SIZE="$(parse_size "$(cat /sys/devices/system/cpu/cpu0/cache/index3/size)")"
  CACHE_SRC="linux-sysfs"

#MAC OS RUNS SYSCTL
elif sysctl -n hw.l1dcachesize >/dev/null 2>&1; then
  L1="$(sysctl -n hw.l1dcachesize 2>/dev/null || echo "")"
  L2="$(sysctl -n hw.l2cachesize 2>/dev/null || echo "")"
  L3="$(sysctl -n hw.l3cachesize 2>/dev/null || echo "")"

  [ -n "$L1" ] && [ "$L1" -gt 0 ] && SYS_L1_CACHE_SIZE="$L1"
  [ -n "$L2" ] && [ "$L2" -gt 0 ] && SYS_L2_CACHE_SIZE="$L2"
  [ -n "$L3" ] && [ "$L3" -gt 0 ] && SYS_L3_CACHE_SIZE="$L3"

  CACHE_SRC="macos-sysctl"

#WINDOW FOR POWERSHELL THX GPT!
elif command -v powershell.exe >/dev/null 2>&1; then
  L1_KB="$(powershell.exe -NoProfile -Command \
    "Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty L1CacheSize" 2>/dev/null || echo "")"
  L2_KB="$(powershell.exe -NoProfile -Command \
    "Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty L2CacheSize" 2>/dev/null || echo "")"
  L3_KB="$(powershell.exe -NoProfile -Command \
    "Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty L3CacheSize" 2>/dev/null || echo "")"

  [ -n "$L1_KB" ] && SYS_L1_CACHE_SIZE="$(( L1_KB * 1024 ))"
  [ -n "$L2_KB" ] && SYS_L2_CACHE_SIZE="$(( L2_KB * 1024 ))"
  [ -n "$L3_KB" ] && SYS_L3_CACHE_SIZE="$(( L3_KB * 1024 ))"

  CACHE_SRC="windows-wmi"
fi

export SYS_L1_CACHE_SIZE
export SYS_L2_CACHE_SIZE
export SYS_L3_CACHE_SIZE

echo "SYSTEM CACHE BUDGET (${CACHE_SRC}):"
echo "SYS_L1_CACHE_SIZE=${SYS_L1_CACHE_SIZE}"
echo "SYS_L2_CACHE_SIZE=${SYS_L2_CACHE_SIZE}"
echo "SYS_L3_CACHE_SIZE=${SYS_L3_CACHE_SIZE}"

# Queue budgets in (0, 1)
# EACH CORE's PINNED QUEUE CAN't GO BEYOND BUDGET*CACHE_SIZE
export L1_L2_QUEUE_BUDGET=0.85 
export L3_QUEUE_BUDGET=0.7

echo "MULTIPLIER FOR CACHE-AWARE QUEUE:"
echo "L1_L2_QUEUE_BUDGET=${L1_L2_QUEUE_BUDGET}"
echo "L3_QUEUE_BUDGET=${L3_QUEUE_BUDGET}"


# MEMORY ALIGNMENT UNIT, IF NOT SURE 
# SET TO 4
export ALIGNMENT_UNIT="$(
  python3 - <<'EOF'
import ctypes
print(ctypes.alignment(ctypes.c_int))
EOF
)"

echo "MEMORY ALIGNMENT UNIT:"
echo "ALIGNMENT_UNIT=${ALIGNMENT_UNIT}"

echo "ENTRY POINT TERMINATION POINT REACHED"

#FROM THE SAME PROCESS
exec "$@"
