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
    STATUS_REPORT = 0xF000

_FN_ID_USER_START = 0x1000
TaskFnTypes = {}

def register_fn_id(name: str, value: int) -> int:
    if value < _FN_ID_USER_START:
        raise ValueError(f"User types must be >= {_FN_ID_USER_START:#04x}")
    if value in ProcTaskFnID.values():
        raise ValueError(f"Queue type value {value:#04x} already registered")
    if name in ProcTaskFnID:
        raise ValueError(f"Queue type name '{name}' already registered")

    TaskFnTypes[name] = value
    return value


#============================================================
# SLOT VARIANT [C1]
#============================================================
class SlotVariant(IntEnum):
    '''
    Distinguishes argument storage strategy in slot types.
    Workers use this to determine how to parse task arguments.
    '''
    INT_ARGS  = 0x00   #args stored as c_int64 array only
    CHAR_ARGS = 0x01   #args include c_char array (c_args field)


#============================================================
# SLOT STRUCTURES
#============================================================
class TaskSlot128(ctypes.Structure):
    '''
    Generic 128-byte task slot with integer arguments.
    
    tsk_id: increment counter distributed to each task
    fn_id: mapping to signify which function worker processes
    args: stores arguments as c_int64 array
    meta: metadata bytes
    
    Variant: INT_ARGS
    '''
    _align_ = 128
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),          # 4 bytes
        ("fn_id", ctypes.c_uint32),           # 4 bytes
        ("args", ctypes.c_int64 * 10),        # 80 bytes generic args
        ("meta", ctypes.c_uint8 * 40),        # 40 bytes meta data
    ]


class TaskSlot196(ctypes.Structure):
    '''
    Generic 192-byte task slot with integer arguments.
    For more flexible usage, refers to TaskSlot256 or TaskSlot512.
    
    Variant: INT_ARGS
    '''
    _align_ = 196
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),          # 4 bytes
        ("fn_id", ctypes.c_uint32),           # 4 bytes
        ("args", ctypes.c_int64 * 15),        # 120 bytes generic args
        ("meta", ctypes.c_uint8 * 64),        # 64 bytes meta data
    ]
    

class TaskSlot128_cargs(ctypes.Structure):
    '''
    128-byte task slot with char+int arguments.
    
    tsk_id: increment counter distributed to each task
    fn_id: mapping to signify which function worker processes
    args: stores integer arguments (reduced count)
    c_args: stores char arguments; use '\0' as termination,
            '0x20' (space) to parse multiple arguments
    meta: metadata bytes
    
    Variant: CHAR_ARGS
    '''
    _align_ = 128
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),          # 4 bytes
        ("fn_id", ctypes.c_uint32),           # 4 bytes
        ("args", ctypes.c_int64 * 2),         # 16 bytes generic args
        ("c_args", ctypes.c_char * 64),       # 64 bytes char args
        ("meta", ctypes.c_uint8 * 40),        # 40 bytes meta data
    ]


class TaskSlot196_cargs(ctypes.Structure):
    '''
    196-byte task slot with char+int arguments.
    
    c_args: stores char arguments; use '\0' as termination,
            '0x20' (space) to parse multiple arguments
    
    Variant: CHAR_ARGS
    '''
    _align_ = 196
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),          # 4 bytes
        ("fn_id", ctypes.c_uint32),           # 4 bytes
        ("args", ctypes.c_int64 * 5),         # 40 bytes generic args
        ("c_args", ctypes.c_char * 84),       # 84 bytes char args
        ("meta", ctypes.c_uint8 * 64),        # 64 bytes meta data
    ]


#============================================================
# SLOTS
#============================================================
SLOT_REGISTRY: dict[Type[ctypes.Structure], SlotVariant] = {
    TaskSlot128:       SlotVariant.INT_ARGS,
    TaskSlot196:       SlotVariant.INT_ARGS,
    TaskSlot128_cargs: SlotVariant.CHAR_ARGS,
    TaskSlot196_cargs: SlotVariant.CHAR_ARGS,
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


def register_slot_class(slot_class: Type[ctypes.Structure], variant: SlotVariant):
    if slot_class in SLOT_REGISTRY:
        raise ValueError(f"slot_class {slot_class.__name__} already registered")
    SLOT_REGISTRY[slot_class] = variant


def get_slot_size(slot_class: Type[ctypes.Structure]) -> int:
    return ctypes.sizeof(slot_class)