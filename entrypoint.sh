#!/bin/sh
# DO NOT USE IT DEPRECIATED BUT KEPT FOR THE POTENTIAL FUTURE USE
#No more needed due to the change in the design, may be used in the future tho.
#No more needed due to the change in the design, may be used in the future tho.
'''
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

#MEMORY ALIGNMENT UNIT, IF NOT SURE SET TO 4
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
'''
