# ============================================================
# ARGS.PY
# ============================================================
# Argument parsing and pool management for task slots.
#
# ArgParser: Parses c_args field from CHAR_ARGS slots
# ArgPool: Manages overflow arguments that don't fit in slot
# ============================================================

import ctypes
from enum import IntEnum
from typing import List, Optional, Tuple, Type
from multiprocessing.shared_memory import SharedMemory
from multiprocessing import Lock

from errors import ErrorCode, Component, format_error


#============================================================
# ARGUMENT PARSER
#============================================================
class ArgParser:
    '''
    Parses c_args field from CHAR_ARGS variant slots.
    
    Default Teriminator and Parsor
        Terminator: 0x00 (null byte)
        Parsor: 0x20 (space)
    
    Example: b"arg1 arg2 arg3\x00" -> [b"arg1", b"arg2", b"arg3"]
    '''
    DEFAULT_TERMINATOR = b'\x00'
    DEFAULT_DELIMITER = b' '  # 0x20
    
    def __init__(self, 
                 terminator: bytes = None,
                 delimiter: bytes = None):
        self.terminator = terminator or self.DEFAULT_TERMINATOR
        self.delimiter = delimiter or self.DEFAULT_DELIMITER
    
    def parse(self, c_args: bytes) -> List[bytes]:
        '''
        Parse c_args bytes into list of arguments.
        
        Args:
            c_args: Raw bytes from slot.c_args field
            
        Returns:
            List of argument bytes (without terminator/delimiter)
        '''
        #Find terminator
        term_idx = c_args.find(self.terminator)
        if term_idx == -1:
            data = c_args
        else:
            data = c_args[:term_idx]
        
        #Split by delimiter
        if not data:
            return []
        
        return data.split(self.delimiter)
    
    def parse_as_str(self, c_args: bytes, encoding: str = 'utf-8') -> List[str]:
        '''Parse c_args and decode to strings.'''
        return [arg.decode(encoding) for arg in self.parse(c_args) if arg]
    
    def pack(self, args: List[bytes]) -> bytes:
        '''
        Compress list of arguments into c_args format.
        
        Args:
            args: List of argument bytes
            
        Returns:
            Packed bytes with delimiter and terminator
        '''
        packed = self.delimiter.join(args)
        return packed + self.terminator
    
    def pack_str(self, args: List[str], encoding: str = 'utf-8') -> bytes:
        return self.pack([arg.encode(encoding) for arg in args])


#============================================================
# ARGUMENT POOL TYPE
#============================================================
class PoolType(IntEnum):
    STATIC = 0   #Pre-allocated fixed slots
    DYNAMIC = 1  #Allocate on demand


#============================================================
# ARGUMENT POOL SLOT
#============================================================
class ArgPoolSlot(ctypes.Structure):
    '''
    Single slot in argument pool.
    Used for arguments too large to fit in task slot.
    '''
    _pack_ = 1
    _fields_ = [
        ("ref_count", ctypes.c_uint16),    # 2 bytes: reference count
        ("data_len", ctypes.c_uint16),     # 2 bytes: actual data length
        ("data", ctypes.c_uint8 * 252),    # 252 bytes: argument data
    ]

ARG_POOL_SLOT_SIZE = ctypes.sizeof(ArgPoolSlot)
assert ARG_POOL_SLOT_SIZE == 256, f"ArgPoolSlot must be 256 bytes, got {ARG_POOL_SLOT_SIZE}"


#============================================================
# ARGUMENT POOL
#============================================================
class ArgPool:
    '''
    Pool for storing arguments that don't fit in task slots.
    
    Static pool: Pre-allocated, fixed number of slots
    Dynamic pool: Grows as needed (TODO: implement resize)
    
    Usage:
    1. Producer stores large arg: pool_id = pool.store(data)
    2. Producer puts pool_id in slot.args[0]
    3. Worker retrieves: data = pool.retrieve(pool_id)
    4. Worker releases when done: pool.release(pool_id)
    '''
    DEFAULT_NUM_SLOTS = 64
    MAX_DATA_SIZE = 252  #Max bytes per slot
    
    def __init__(self,
                 pool_type: PoolType = PoolType.STATIC,
                 num_slots: int = None,
                 shared: bool = True):
        '''
        Initialize argument pool.
        
        Arguments:
            pool_type: STATIC or DYNAMIC
            num_slots: Number of slots (default: 64)
            shared: If True, use SharedMemory for cross-process
        '''
        self.pool_type = pool_type
        self._num_slots = num_slots or self.DEFAULT_NUM_SLOTS
        self._shared = shared
        
        self._free_bitmap = (1 << self._num_slots) - 1
        
        buffer_size = self._num_slots * ARG_POOL_SLOT_SIZE
        
        if shared:
            self._shm = SharedMemory(create=True, size=buffer_size)
            SlotArray = ArgPoolSlot * self._num_slots
            self._slots = SlotArray.from_buffer(self._shm.buf)
            self.shm_name = self._shm.name
        else:
            SlotArray = ArgPoolSlot * self._num_slots
            self._slots = SlotArray()
            self.shm_name = None
        
        for i in range(self._num_slots):
            self._slots[i].ref_count = 0
            self._slots[i].data_len = 0
        
        self._lock = Lock() if shared else None
    
    def store(self, data: bytes) -> int:
        if len(data) > self.MAX_DATA_SIZE:
            raise ValueError(format_error(
                ErrorCode.E004_INVALID_BATCH_SIZE,
                Component.ALLOCATION,
                f"Arg data too large: {len(data)} > {self.MAX_DATA_SIZE}"
            ))
        
        slot_id = self._alloc_slot()
        if slot_id < 0:
            return -1  #Pool full
        
        slot = self._slots[slot_id]
        slot.ref_count = 1
        slot.data_len = len(data)
        ctypes.memmove(ctypes.byref(slot.data), data, len(data))
        
        return slot_id
    
    def retrieve(self, slot_id: int) -> Optional[bytes]:
        if slot_id < 0 or slot_id >= self._num_slots:
            return None
        
        slot = self._slots[slot_id]
        if slot.ref_count == 0:
            return None  #Slot not allocated
        
        return bytes(slot.data[:slot.data_len])
    
    def add_ref(self, slot_id: int):
        if 0 <= slot_id < self._num_slots:
            self._slots[slot_id].ref_count += 1
    
    def release(self, slot_id: int):
        if slot_id < 0 or slot_id >= self._num_slots:
            return
        
        slot = self._slots[slot_id]
        if slot.ref_count > 0:
            slot.ref_count -= 1
            if slot.ref_count == 0:
                self._free_slot(slot_id)
    
    def _alloc_slot(self) -> int:
        if self._lock:
            self._lock.acquire()
        
        try:
            if self._free_bitmap == 0:
                return -1  #No free slots
            
            #Find first free bit
            slot_id = (self._free_bitmap & -self._free_bitmap).bit_length() - 1
            
            #Mark as allocated
            self._free_bitmap &= ~(1 << slot_id)
            
            return slot_id
        finally:
            if self._lock:
                self._lock.release()
    
    def _free_slot(self, slot_id: int):
        if self._lock:
            self._lock.acquire()
        
        try:
            self._free_bitmap |= (1 << slot_id)
        finally:
            if self._lock:
                self._lock.release()
    
    def get_free_count(self) -> int:
        return bin(self._free_bitmap).count('1')
    
    def cleanup(self):
        if self._shared and hasattr(self, '_shm'):
            try:
                self._shm.close()
                self._shm.unlink()
            except:
                pass


#============================================================
# HELPERS
#============================================================
def create_arg_parser(terminator: bytes = b'\x00', 
                      delimiter: bytes = b' ') -> ArgParser:
    return ArgParser(terminator=terminator, delimiter=delimiter)


def pack_args(*args, encoding: str = 'utf-8') -> bytes:
    parser = ArgParser()
    return parser.pack_str(list(args), encoding)


def unpack_args(c_args: bytes, encoding: str = 'utf-8') -> List[str]:
    parser = ArgParser()
    return parser.parse_as_str(c_args, encoding)