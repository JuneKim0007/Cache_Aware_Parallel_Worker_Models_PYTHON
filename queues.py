import ctypes
from enum import IntEnum
from slot import Slot


#============================================================
# QUEUE TYPE
# Classification of Queue's Role
# similar to '#DEFINE KEY VALUE' in C
# <EVERYTHING IS INDEEN AN OBJECT IN PYTHON>
#============================================================
from enum import IntEnum

class QueueType(IntEnum):
    """
    Queue Type Registry. 
    User Types (0x0100+):
        Available for custom queue types
    Queue Types FROM 0x000 TO 0x0ff are RESERVED.
    To add custom types:

        QueueType.<MY_CUSTOM> = 0x0100
        or
        class MyTypes(QueueType):
            PRIORITY = 0x0100
        or 
        QueueType.register(cls, name, value)
            Refer to QueueType.register()
    """
    TASK_DISTRIBUTE = 0x0000
    TASK_DISPATCH   = 0x0001
    L2_BATCH        = 0x0002
    L3_IO           = 0x0003
    
    _USER_START = 0x0100
    
    #maybe in the future i can add args and kargs for convinent implementation.
    @classmethod
    def register_user_type(cls, name: str, value: int | None = None):
        """
        A function to register a new custom queue type.
    
        Args:
            cls: Class object to add a type mapping.
            name: Type name. 
            value: Type ID (must be >= 0x0100), or leave it for auto assigning.
        
        Returns:
            The assigned type ID
        
        Example:
            QueueType.register_user_type('MY_QUEUE')  # Auto-assigns
            QueueType.register_user_type('CUSTOM', 0x0150)
        """
        if value is None:
            # Auto-assign with attacing 0x100 since it auto() may invade reserved area
            existing = [v for v in cls.__members__.values() if v >= cls._USER_START]
            value=max(existing, default=cls._USER_START- 1)+ 1
        
        if value < cls._USER_START:
            raise ValueError(f"[VALUE ERR] Expected type {name} must have value >= {cls._USER_START:#06x}, got {value:#06x}")
        
        if name in cls.__members__:
            raise ValueError(f"Queue type {name} already exists")
        
        cls._member_map_[name] = value
        cls._value2member_map_[value] = cls._member_map_[name]
        
        return value


#============================================================
# QUEUE CONFIG
# COLD EXTERNAL STATE
# Metadata (SOURCE OF TRUTH)
#============================================================

class QueueConfig(ctypes.Structure):
    '''
    SOURCE OF TRUTH
    Often only to be explictly called. (Almost no internal access)
    Mainly for adminstrating, debugging purpose.
    '''
    _fields_ = [
        ("size", ctypes.c_uint32),        # number of slots
        ("slot_size", ctypes.c_uint32),   # sizeof(Slot)
        ("queue_type", ctypes.c_uint32),
    ]


#============================================================
# HOT, INTERNAL STATE (CURSOR)
#============================================================
class QueueState:
    '''
    DO NOT MODIFY VALUES UNLESS YOU KNOW WHAT YOU ARE DOING
    THOSE VARIABLES ARE FOR INTERNAL ACCSSE ONLY.
    Head: the first valid index of an array of Slot
    Tail: the last valid index
    size: the number of slots
    '''
    __slots__ = ("head", "tail", "_size", "mask")

    def __init__(self, size: int):
        self.head = 0
        self.tail = 0
        self._size = size          # number of slots (fixed)
        self.mask = size - 1       # for fast wraparound

    #capacity should be immutable.
    #this property setter might not needed with careful coding. Just a safeguard.
    @property
    def size(self) -> int:
        return self._size


# ============================================================
# Cache-aware Single-Producer / Single-Consumer Queue
# ============================================================
class LocalTaskQueue:
    #docstring
    """
    Lock-free, Circular Queue 'local' to assigned workers for task-requests managing.
    Single producer and consumer model.
    HOT variables: Variables that are frequently accessed.  
        _state: Refers to a QueueState instance [head, tail, size]
        slots: Refers to an array of Slots
    COLD variables : Variables that are not frequently accessed. Often only accessed if EXPLICITLY called
        _config: Refers to a QueueType instance [size, slot_size, queue_type]
        _raw_buffer: PLACE HOLDER VALUES FOR MEMORY ALLOCATION, DO NOT MODIFY THIS

    Memory Layout:
        Slots aligned to 64-byte cache line boundaries
        Base slot size: 192 bytes (3 * 64-byte cache lines) for Cache-line awareness
        Contiguous allocation for locality and faster access.
    DESINING RATIONALE:
        AVOID USING Ctypes unless needed to stay "HOT" in L1/L2 Cache.
        Other less frequent datas not necessarily needed to stay "Hot" will be of __slot__ class to avoid Python overhead.
    """
    
    
    __slots__ = (
        "_state",           # HOT: Frequently accessed variables
        "_slots_base",
        "slots",
        "_config",          # COLD: those data won't be accessed unless EXPLICITLY called ostly for Layout
        "_raw_buffer",
    )
    #JUST FOR THE VISIBILITY, MAYBE I SHOULD MAKE size : int = 128. 
    DEFAULT_SIZE = 128  # DEFAULT SIZE MUST BE POWER OF TWO

    def __init__(self, size: int | None = None,
                 qtype: QueueType = QueueType.TASK_DISPATCH):

        size = size or self.DEFAULT_SIZE
        if size & (size - 1) != 0:
            raise ValueError("Queue size must be power of two")

        #COLD
        self._config = QueueConfig(
            size=size,
            slot_size=ctypes.sizeof(Slot),
            queue_type=qtype,
        )

        #PLACE HOLDER FOR MEMORY LAYOUT
        raw_bytes = size* self._config.slot_size+ 63
        self._raw_buffer= ctypes.create_string_buffer(raw_bytes)
        raw_addr = ctypes.addressof(self._raw_buffer)
        self._slots_base= (raw_addr + 63)&~63  # 64B alignment

        SlotArray = Slot* size
        self.slots =SlotArray.from_address(self._slots_base)

        self._state =QueueState(size)

    #Not really necessary but for safe guarding
    @property
    def size(self) -> int:
        return self._config.size

    @property
    def layout(self) -> QueueConfig:
        return self._config

    #UTILITY FUNCTIONS
    def enqueue(self, task_id: int):
        state = self._state
        slots = self.slots

        tail =state.tail
        next_tail = (tail + 1) &state.mask

        if next_tail== state.head:
            raise RuntimeError("Queue full")

        slots[tail].task_id = task_id
        state.tail = next_tail

    def dequeue(self)-> int:
        state =self._state
        slots =self.slots

        head =state.head
        if head ==state.tail:
            raise RuntimeError("Queue empty")

        task_id = slots[head].task_id
        state.head =(head +1) & state.mask
        return task_id

    def is_empty(self) -> bool:
        s = self._state
        return s.head ==s.tail

    def is_full(self) -> bool:
        s = self._state
        return ((s.tail+ 1) &s.mask) ==s.head

    def count(self) -> int:
        s = self._state
        return (s.tail -s.head) &s.mask
