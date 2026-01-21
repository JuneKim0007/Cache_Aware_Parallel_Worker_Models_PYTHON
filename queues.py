import ctypes
from enum import IntEnum
from slot import TaskSlot128
import threading

#============================================================
# QUEUE TYPE
#============================================================
class ProcQueueTypes(IntEnum):
    TASK_DISTRIBUTE = 0x0000
    TASK_DISPATCH = 0x0101  # Added as example for LocalTaskQueue

_QUEUE_TYPE_USER_START = 0x0100
QueueTypes = {}

def register_queue_type(name: str, value: int):
    if value < _QUEUE_TYPE_USER_START:
        raise ValueError(f"User types must be >= {_QUEUE_TYPE_USER_START:#04x}")
    if value in QueueTypes.values():
        raise ValueError(f"Queue type value {value:#04x} already registered")
    if name in QueueTypes:
        raise ValueError(f"Queue type name '{name}' already registered")
    QueueTypes[name] = value
    return value

#============================================================
# QUEUE CONFIG
#============================================================
class QueueConfig(ctypes.Structure):
    _fields_ = [
        ("num_slots", ctypes.c_uint32),      # renamed size -> num_slots
        ("slot_size", ctypes.c_uint32),
        ("queue_type", ctypes.c_uint32),
    ]

#============================================================
# QUEUE STATE
#============================================================
class QueueState:
    __slots__ = ("head", "tail", "_num_slots", "mask")
    def __init__(self, num_slots: int):
        self.head = 0
        self.tail = 0
        self._num_slots = num_slots
        self.mask = num_slots - 1

    @property
    def num_slots(self) -> int:
        return self._num_slots

# ============================================================
# Cache-aware Single-Producer / Single-Consumer Queue
# ============================================================
class LocalTaskQueue:
    __slots__ = (
        "_state",
        "_slots_base",
        "slots",
        "_config",
        "_raw_buffer",
    )
    DEFAULT_NUM_SLOTS = 256  # must be power of two

    def __init__(self, num_slots: int | None = None,
                 slot = TaskSlot128,
                 qtype: ProcQueueTypes = ProcQueueTypes.TASK_DISPATCH):
        
        num_slots = num_slots or self.DEFAULT_NUM_SLOTS
        self._config = QueueConfig(
            num_slots=num_slots,
            slot_size=ctypes.sizeof(slot),
            queue_type=qtype,
        )
        raw_bytes =num_slots * self._config.slot_size+ 63
        self._raw_buffer = ctypes.create_string_buffer(raw_bytes)
        raw_addr =ctypes.addressof(self._raw_buffer)
        self._slots_base = (raw_addr + 63)& ~63

        SlotArray =TaskSlot128 *num_slots
        self.slots =SlotArray.from_address(self._slots_base)

        self._state = QueueState(num_slots)

        print(f"[QUEUE][LOCAL][INITIALIZED] onto Worker 'workerID' on 'TS'")

    @property
    def num_slots(self) -> int:
        return self._config.num_slots

    @property
    def config(self) -> QueueConfig:
        return self._config

    #since object is allocated, just put the value
    def enqueue(self, task_id: int):
        state = self._state
        slots = self.slots

        tail = state.tail
        next_tail = (tail + 1) & state.mask

        if next_tail == state.head:
            raise RuntimeError("Queue full")

        slots[tail].task_id = task_id
        state.tail = next_tail

    #Security is not a concern no need to zerorize
    def dequeue(self) -> int:
        state = self._state
        slots = self.slots

        head = state.head
        if head == state.tail:
            raise RuntimeError("Queue empty")

        task_id = slots[head].task_id
        state.head = (head + 1) &state.mask
        return task_id

    def is_empty(self) -> bool:
        s = self._state
        return s.head == s.tail

    def is_full(self) -> bool:
        s = self._state
        return ((s.tail + 1) & s.mask) ==s.head

    def count(self) -> int:
        s = self._state
        return (s.tail - s.head) &s.mask

# ============================================================
# SharedTaskQueue with identical structure style to LocalTaskQueue
# ============================================================
class SharedTaskQueue:
    '''
    queue_id : int
    num_slots: int
    RIGHT NOW Slot is predifned manually
    '''
    def __init__(self, queue_id : int, num_slots: int):

        self._config = QueueConfig(
            num_slots=num_slots,
            slot_size=ctypes.sizeof(TaskSlot128),
            queue_type=ProcQueueTypes.TASK_DISTRIBUTE,
        )

        self.queue_id = queue_id
        raw_bytes = num_slots * self._config.slot_size +63
        self._raw_buffer = ctypes.create_string_buffer(raw_bytes)
        raw_addr = ctypes.addressof(self._raw_buffer)
        self._slots_base = (raw_addr + 63) &~63

        SlotArray = TaskSlot128 * num_slots
        self.slots = SlotArray.from_address(self._slots_base)

        self._state = QueueState(num_slots)
        self.lock = threading.Lock()
        print(f"[QUEUE][SHARED][INITIALIZED] queue_id = {self.queue_id} on 'TS'")

    @property
    def num_slots(self) -> int:
        return self._config.num_slots
    @property
    def config(self) -> QueueConfig:
        return self._config

    def enqueue(
        self, fn_id: int, tsk_id: int,
        args, #ARGS MUST BE C_uint64 type!
        meta  #META MUST BE C_uint8 *40 type!
    ):
        '''
        For 64 byte
        ("tsk_id", ctypes.c_uint32),           # 4 bytes
        ("fn_id", ctypes.c_uint32),          # 4 bytes
        ("args", ctypes.c_uint64 * 2),        # 16 bytes generic args
        ("meta", ctypes.c_uint8 * 40),        # 40 bytes auxiliary data
        '''
        state =self._state
        slots =self.slots

        tail =state.tail
        next_tail = (tail + 1) & state.mask

        if next_tail == state.head:
            raise RuntimeError("Queue full")

        slots[tail].tsk_id = tsk_id
        slots[tail].fn_id = fn_id
        slots[tail].args = args
        slots[tail].meta = meta 
        state.tail = next_tail


    #RIGHT NOW JUST COPY AND PASTED TO BE IMPLEMENTED ACCORDINLGLY 
    def dequeue(self):
        state = self._state
        slots = self.slots

        head = state.head
        if head == state.tail:
            raise RuntimeError("Queue empty")

        task_id = slots[head].task_id
        state.head = (head + 1) &state.mask
        return task_id

    def is_empty(self) -> bool:
        s = self._state
        return s.head == s.tail

    def is_full(self) -> bool:
        s = self._state
        return ((s.tail + 1) & s.mask) == s.head

    def count(self) -> int:
        s = self._state
        return (s.tail - s.head) & s.mask
    
    def __str__(self):
        state = self._state
        info=[
            f"[SharedTaskQueue] queue_id={self.queue_id}",
            f"num_slots={self.num_slots}",
            f"head={state.head}, tail={state.tail}, count={self.count()}",
            f"full={self.is_full()}, empty={self.is_empty()}",
        ]

        tasks = []
        #print ten right now
        for i in range(10):
            idx = (state.head + i)& state.mask
            slot = self.slots[idx]
            tasks.append(f"idx={idx}: tsk_id={slot.tsk_id}, fn_id={slot.fn_id}")


        return "\n".join(info)


#
#
#
#
#
#FOR TESTING QUEUE
#
#
def alloc_testing() -> tuple:
    a = SharedTaskQueue(1,128)
    b = LocalTaskQueue()
    return (a,b)

#alloc has to be successful anyway
def enqueue_testing():
    c, d = alloc_testing()
    ArgArray2 = ctypes.c_uint64 * 2
    arg1 = ctypes.c_uint64 * 2
    arg1 = ArgArray2()
    arg1[0] = 123
    arg1[1] = 456
    MetaArray = ctypes.c_uint8 * 40
    meta = MetaArray()
    c.enqueue(1,2,
              arg1,meta)
    print(c)
    

if __name__ == "__main__":
    #alloc_testing()
    enqueue_testing()