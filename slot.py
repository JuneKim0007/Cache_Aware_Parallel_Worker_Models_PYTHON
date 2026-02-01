
import ctypes
from enum import IntEnum

#Maybe loading from .json file might be nicer.
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
    This is a generic 128-byte task slot to hold task_id, function_id, arguments and meta data.
    For more flexible usage, refers to TaskSlot196 or 256. 
    tsk_id: tsk_id is an increment counter distributed to each task.
    fn_id: fn_id is a mapping to signify which function worker has to process.
    args: stores arguments for a fuction that is mapped to fn_id. 
    '''
    _align_ = 128 #make sure its a multiple of 64 bytes.
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),           # 4 bytes
        ("fn_id", ctypes.c_uint32),          # 4 bytes
        ("args", ctypes.c_int64 * 10),        # 80 bytes generic args
        ("meta", ctypes.c_uint8 * 40),        # 40 bytes meta data
    ]



class TaskSlot196(ctypes.Structure):
    '''
    This is a generic 192-byte task slot to hold task_id, function_id, arguments and meta data.
    For more flexible usage, refers to TaskSlot256 or TaskSlot512

    tsk_id: tsk_id is an increment counter distributed to each task.
    fn_id: fn_id is a mapping to signify which function worker has to process.
    args: stores arguments for a fuction that is mapped to fn_id. 
    '''
    _align_ = 196 #make sure its a multiple of 64 bytes.
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),           # 4 bytes
        ("fn_id", ctypes.c_uint32),          # 4 bytes
        ("args", ctypes.c_int64 * 15),        # 120 bytes generic args
        ("meta", ctypes.c_uint8 * 64),        # 64 bytes meta data
    ]
    
class TaskSlot128_cargs(ctypes.Structure):
    '''
    This char+int arugment 128-byte task slot to hold task_id, function_id, arguments and meta data.
    For more flexible usage, refers to TaskSlot256 or TaskSlot512

    tsk_id: tsk_id is an increment counter distributed to each task.
    fn_id: fn_id is a mapping to signify which function worker has to process.
    args: stores arguments for a fuction that is mapped to fn_id. 
    c_args: stores argument in char type. Use '\0' as a termination condition. 
        and '0x20' or white space to parse arguments.

        d
    '''
    _align_ = 128 #make sure its a multiple of 64 bytes.
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),          # 4 bytes
        ("fn_id", ctypes.c_uint32),           # 4 bytes
        ("args", ctypes.c_int64 * 2),        # 16 bytes generic args
        ("c_args", ctypes.c_char * 64),       # 64 bytes char args
        ("meta", ctypes.c_uint8 * 40),        # 40 bytes meta data
    ]



class TaskSlot196_cargs(ctypes.Structure):
    '''
    This char+int arugment 196-byte task slot to hold task_id, function_id, arguments and meta data.
    For more flexible usage, refers to TaskSlot256 or TaskSlot512

    tsk_id: tsk_id is an increment counter distributed to each task.
    fn_id: fn_id is a mapping to signify which function worker has to process.
    args: stores arguments for a fuction that is mapped to fn_id. 
    c_args: stores argument in char type. Use '\0' as a termination condition. 
            and '0x20' or white space to parse arguments.

    '''
    _align_ = 196 #make sure its a multiple of 64 bytes.
    _fields_ = [
        ("tsk_id", ctypes.c_uint32),          # 4 bytes
        ("fn_id", ctypes.c_uint32),           # 4 bytes
        ("args", ctypes.c_int64 * 5),         # 40 bytes generic args
        ("c_args", ctypes.c_char * 84),       # 84 bytes char args
        ("meta", ctypes.c_uint8 * 64),        # 64 bytes meta data
    ]

    ####
