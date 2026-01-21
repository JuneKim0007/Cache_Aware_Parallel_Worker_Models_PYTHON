
import ctypes
from enum import IntEnum

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

class TaskSlot128(ctypes.Structure):
    '''
    This is a generic 192-byte task slot to hold task_id, function_id, arguments and meta data.
    For more flexible usage, refers to TaskSlot256 or TaskSlot512

    tsk_id: tsk_id is an increment counter distributed to each task.
    fn_id: fn_id is a mapping to signify which function worker has to process.
    
    args: stores arguments for a fuction that is mapped to fn_id. 
    IT MUST NOT TAKE ARGUMENTS FROM ANY DECORATOR FUNCTIONS!!
    It is often recommended to create a function that is already decorated and create a mapping to fn_id.
    Refer to register_fn_id()

    
    #META DATA SECTION
    (log: if set to 1, the work will be logged)
    (ts: if set to 1, the log will contain time stamp. It will be ignore if log = 0)
    ()
    meta: meta data holds args about:
        d
    '''
    _align_ = 64
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),           # 4 bytes
        ("fn_id", ctypes.c_uint32),          # 4 bytes
        ("args", ctypes.c_uint64 * 2),        # 16 bytes generic args
        ("meta", ctypes.c_uint8 * 40),        # 40 bytes auxiliary data
    ]


