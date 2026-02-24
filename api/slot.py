# ============================================================
# API/SLOT.PY — Universal 128-byte task slot
# ============================================================
# Single slot layout, no variants. 8-byte aligned fields.
#
# ┌──────────┬──────────┬──────────────────┬──────────────┐
# │ tsk_id 8B│ fn_id 8B │   args 80B       │   meta 32B   │ = 128B
# │ uint64   │ uint64   │  10 × int64      │              │
# └──────────┴──────────┴──────────────────┴──────────────┘
# ============================================================

import ctypes
from enum import IntEnum


# ============================================================
# INTERNAL FUNCTION IDS (0x0000 - 0x0FFF)
# User space starts at 0x1000
# ============================================================
class ProcTaskFnID(IntEnum):
    TERMINATE     = 0x0000
    INCREMENT     = 0x0210
    ADD           = 0x0220
    MULTIPLY      = 0x0230
    STATUS_REPORT = 0x0F00


# ============================================================
# UNIVERSAL TASK SLOT — 128 bytes, cache-line friendly
# ============================================================
class TaskSlot(ctypes.Structure):

    _pack_ = 8
    _fields_ = [
        ("tsk_id", ctypes.c_uint64),        # 8B  — debug/tracking ID
        ("fn_id",  ctypes.c_uint64),         # 8B  — function identifier
        ("args",   ctypes.c_int64 * 10),     # 80B — 10 argument slots
        ("meta",   ctypes.c_uint8 * 32),     # 32B — metadata / pool refs
    ]

assert ctypes.sizeof(TaskSlot) == 128, f"TaskSlot must be 128B, got {ctypes.sizeof(TaskSlot)}"


# ============================================================
# CONSTANTS
# ============================================================
SLOT_CLASS_MAP = {"TaskSlot": TaskSlot}
SLOT_SIZE = ctypes.sizeof(TaskSlot)
MAX_ARGS = 10