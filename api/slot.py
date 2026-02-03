# ============================================================
# API/SLOT.PY
# ============================================================
# Task slot structures and function IDs.
# ============================================================

import ctypes
from enum import IntEnum
from typing import Type


#============================================================
# TASK FUNCTION ID
#============================================================
class ProcTaskFnID(IntEnum):
    TERMINATE     = 0x0000
    READ_FILE     = 0x1000
    READ_DIR      = 0x1100
    HASH          = 0x2000
    INCREMENT     = 0x2100
    ADD           = 0x2200
    MULTIPLY      = 0x2300
    STATUS_REPORT = 0xF000


#============================================================
# SLOT VARIANT
#============================================================
class SlotVariant(IntEnum):
    INT_ARGS  = 0x00
    CHAR_ARGS = 0x01


#============================================================
# SLOT STRUCTURES
#============================================================
class TaskSlot128(ctypes.Structure):
    '''128-byte task slot with integer arguments.'''
    _align_ = 128
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),
        ("fn_id", ctypes.c_uint32),
        ("args", ctypes.c_int64 * 10),
        ("meta", ctypes.c_uint8 * 40),
    ]


class TaskSlot196(ctypes.Structure):
    '''196-byte task slot with integer arguments.'''
    _align_ = 196
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),
        ("fn_id", ctypes.c_uint32),
        ("args", ctypes.c_int64 * 15),
        ("meta", ctypes.c_uint8 * 64),
    ]


class TaskSlot128_cargs(ctypes.Structure):
    '''128-byte task slot with char+int arguments.'''
    _align_ = 128
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),
        ("fn_id", ctypes.c_uint32),
        ("args", ctypes.c_int64 * 2),
        ("c_args", ctypes.c_char * 64),
        ("meta", ctypes.c_uint8 * 40),
    ]


class TaskSlot196_cargs(ctypes.Structure):
    '''196-byte task slot with char+int arguments.'''
    _align_ = 196
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),
        ("fn_id", ctypes.c_uint32),
        ("args", ctypes.c_int64 * 5),
        ("c_args", ctypes.c_char * 84),
        ("meta", ctypes.c_uint8 * 64),
    ]


#============================================================
# SLOT REGISTRY
#============================================================
SLOT_REGISTRY: dict[Type[ctypes.Structure], SlotVariant] = {
    TaskSlot128:       SlotVariant.INT_ARGS,
    TaskSlot196:       SlotVariant.INT_ARGS,
    TaskSlot128_cargs: SlotVariant.CHAR_ARGS,
    TaskSlot196_cargs: SlotVariant.CHAR_ARGS,
}

SLOT_CLASS_MAP = {
    "TaskSlot128": TaskSlot128,
    "TaskSlot196": TaskSlot196,
    "TaskSlot128_cargs": TaskSlot128_cargs,
    "TaskSlot196_cargs": TaskSlot196_cargs,
}


#============================================================
# HELPERS
#============================================================
def get_slot_variant(slot_class: Type[ctypes.Structure]) -> SlotVariant:
    if slot_class not in SLOT_REGISTRY:
        raise KeyError(f"slot_class {slot_class.__name__} not in SLOT_REGISTRY")
    return SLOT_REGISTRY[slot_class]


def has_char_args(slot_class: Type[ctypes.Structure]) -> bool:
    return get_slot_variant(slot_class) == SlotVariant.CHAR_ARGS


def get_slot_size(slot_class: Type[ctypes.Structure]) -> int:
    return ctypes.sizeof(slot_class)