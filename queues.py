import ctypes
from enum import IntEnum
from slot import TaskSlot128
import threading

class AllocationError(Exception):
    pass
#
#
#
#
#
#


#============================================================
# QUEUE TYPE
#============================================================
class ProcQueueTypes(IntEnum):
    TASK_DISTRIBUTE = 0x0000
    TASK_DISPATCH = 0x0001

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
        ("_num_slots", ctypes.c_uint32),      # renamed size -> _num_slots
        ("slot_size", ctypes.c_uint32),
        ("queue_type", ctypes.c_uint32),
        ("num_workers", ctypes.c_uint8)
    ]

#============================================================
# QUEUE STATE
#============================================================
class QueueState:
    '''
    head: start idx for occupied slots
    tail: tail idx for occupied slots
    _num_slots: number of all slots regardless of their state
    curr_size: number of occupoed slots
    mask: used for quicker modular arthimetic
    --For shared queue only
    _batch_bitmap: internal variable to indicate which worker is participating in batching.
    _batch_accumulation: internal variable to indicate how much head has to be moved forward once global dequeuing completes.
    '''
    __slots__ = ("head", "tail", "_num_slots", "curr_size", "mask", "_batch_bitmap", "_batch_accumulation")
    def __init__(self, _num_slots: int):
        self.head: int = 0
        self.tail: int = 0
        self._num_slots: int = _num_slots
        #curr_size represents number of occupied slots
        #mostly helps queue limit checking
        self.curr_size: int = 0
        self.mask :int = _num_slots - 1

        #each time batch is requested, worker
        #mitigation plan is to have a worker that signals once batch_list ==0
        self._batch_bitmap = ctypes.c_uint64(0)
        self._batch_accumulation: int =0


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

    def __init__(self, _num_slots: int | None = None,
                 slot = TaskSlot128,
                 qtype: ProcQueueTypes = ProcQueueTypes.TASK_DISPATCH):
        
        #DEFAULT SLOT
        _num_slots = _num_slots or self.DEFAULT_NUM_SLOTS

        #ALLOCATION SECTION:

        #COLD [CONFIG]
        self._config = QueueConfig(
            _num_slots=_num_slots,
            slot_size=ctypes.sizeof(slot),
            queue_type=qtype,
            num_workers = 1
        )
        #MEMORY LAYOUT
        raw_bytes =_num_slots * self._config.slot_size+ 63
        self._raw_buffer = ctypes.create_string_buffer(raw_bytes)
        raw_addr =ctypes.addressof(self._raw_buffer)
        self._slots_base = (raw_addr + 63)& ~63

        SlotArray =TaskSlot128 *_num_slots
        self.slots =SlotArray.from_address(self._slots_base)
        #HOT [STATE] 
        self._state = QueueState(_num_slots)

        print(f"[QUEUE][LOCAL][INITIALIZED] onto Worker 'workerID' on 'TS'")

    @property
    def config(self) -> QueueConfig:
        return self._config

    #since object is allocated, just put the value
    def enqueue(self, slot):
        
        state =self._state
        slots =self.slots
        tail = state.tail

        #for % modular arthimetics
        next_tail = (tail +1) &state.mask
        
        #Right now it simply rejects but in the future it has to take avaiable amounts
        if state.curr_size == state._num_slots:
            raise RuntimeError("Queue full")
        
        ctypes.memmove(ctypes.byref(slots[tail]), ctypes.byref(slot),ctypes.sizeof(slot))
        #UPDATE HOT [STATE]
        self._state.tail =next_tail
        self._state.curr_size += 1


    #Security is not a concern no need to zerorize
    def dequeue(self) -> int:
        state = self._state
        slots = self.slots

        head = state.head
        if state.curr_size == 0:
            raise RuntimeError("Queue empty")

        tsk_id = slots[head].tsk_id
        #UPDATE HOT [STATE]
        self._state.head = (head + 1) &state.mask
        self._state.curr_size -=1
        return tsk_id

    def batch_enqueue(self, count, slot):
        '''
        count: number of slots to copy
        slot: has to be the first slot
        '''
        state =self._state
        slots =self.slots

        tail = state.tail
        next_tail = (tail +count) &state.mask

        available_slots = self._state._num_slots - self._state.curr_size 
        #RIGHT NOW IT JUST REJECTS THE WHOLE
        #but in the future, it will try to fetch the maximum possible slots
        if available_slots < count:
            raise RuntimeError(f"[ERROR][Queue][Local] onto Worker 'workerid' - Queue full")
        #just copy everything from the given slot
        first = min(count, state._num_slots - tail)
        second = count - first

        ctypes.memmove(
            ctypes.byref(slots[tail]),
            ctypes.byref(slot),
            ctypes.sizeof(slot) * first
        )

        if second > 0:
            ctypes.memmove(
                ctypes.byref(slots[0]),
                ctypes.byref(slot, ctypes.sizeof(slot) * first),
                ctypes.sizeof(slot) * second
            )

        self._state.tail =next_tail

        self._state.curr_size += count



    #
    #   UTILS
    #
    def is_empty(self) -> bool:
        s = self._state
        return s.head == s.tail

    def is_full(self) -> bool:
        available_slots = self._state._num_slots - self._state.curr_size
        return (available_slots <1)

    def count(self) -> int:
        return self._state.curr_size
    
    def __str__(self):
        state = self._state
        info=[
            f"[LOCAL TASK QUEUE]",
            f"head={state.head}, tail={state.tail}, count={self.count()}",
            f"full={self.is_full()}, empty={self.is_empty()}",
        ]

        tasks = []
        #print ten right now
        for i in range(10):
            idx = (state.head + i)& state.mask
            slot = self.slots[idx]
            tasks.append(f"idx={idx}: tsk_id={slot.tsk_id}, fn_id={slot.fn_id}")

        info.extend(tasks)
        return "\n".join(info)

#============================================================
# SharedTaskQueue
#============================================================
class SharedTaskQueue:
    '''
    queue_id : int
    _num_slots: int
    RIGHT NOW Slot is predifned manually
    '''
    def __init__(self, queue_id : int, _num_slots: int, num_workers: int):


        #Allocation section
        if num_workers > 64:
            raise AllocationError(f"ERROR ALLOC num_worekres> 64, got {num_workers}")

        self._config = QueueConfig(
            _num_slots=_num_slots,
            slot_size=ctypes.sizeof(TaskSlot128),
            queue_type=ProcQueueTypes.TASK_DISTRIBUTE,
            num_workers = num_workers
        )

        self.queue_id = queue_id
        raw_bytes = _num_slots * self._config.slot_size +63
        self._raw_buffer = ctypes.create_string_buffer(raw_bytes)
        raw_addr = ctypes.addressof(self._raw_buffer)
        self._slots_base = (raw_addr + 63) &~63

        SlotArray = TaskSlot128 * _num_slots
        self.slots = SlotArray.from_address(self._slots_base)

        self._state = QueueState(_num_slots)
        self.lock = threading.Lock()
        print(f"[QUEUE][SHARED][INITIALIZED] queue_id = {self.queue_id} on 'TS'")

    @property
    def _num_slots(self) -> int:
        return self._config._num_slots
    @property
    def config(self) -> QueueConfig:
        return self._config

    def enqueue(self, slot):
        state = self._state
        slots = self.slots

        tail = state.tail
        next_tail = (tail + 1) & state.mask

        available_slots = self._state._num_slots - self._state.curr_size 
        # Right now it simply rejects but in the future it has to take available amounts
        if available_slots < 1:
            raise RuntimeError(f"[Queue][Shared][ERROR] Queue Id: {self.queue_id}- Queue full")

        # Scheduler must guarantee data within slot is strictly valid
        # also the worker sees corrupted task then just raise error and proceed to next work.
        # just copy everything from the given slot into the tail position
        ctypes.memmove(ctypes.byref(slots[tail]), ctypes.byref(slot), ctypes.sizeof(slot))


        #update once copy is done to avoid workers reading incomeplete slot.
        self._state.curr_size += 1
        self._state.tail = next_tail


    def dequeue(self):
        state = self._state
        slots = self.slots

        head = state.head
        if head == state.tail:
            raise RuntimeError("Queue empty")

        tsk_id = slots[head].tsk_id
        self._state.head = (head + 1) &state.mask

        self._state.curr_size -=1
        return tsk_id

    def is_empty(self) -> bool:
        s = self._state
        return s.head == s.tail
    
    def __str__(self):
        state = self._state
        info=[
            f"[SharedTaskQueue] queue_id={self.queue_id}",
            f"_num_slots={self._num_slots}",
            f"head={state.head}, tail={state.tail}, empty={self.is_empty()}",
        ]

        tasks = []
        #print ten right now
        for i in range(0,10):
            idx = (state.head + i)& state.mask
            slot = self.slots[idx]
            tasks.append(f"idx={idx}: tsk_id={slot.tsk_id}, fn_id={slot.fn_id}")
        info.extend(tasks)
        return "\n".join(info)


#
#
#
#
#
#FOR DEBUGGING ONLY
#
#
def print_state(queue):
    state = queue._state
    print(f"Queue State:")
    print(f"  head = {state.head}")
    print(f"  tail = {state.tail}")
    print(f"  curr_size = {state.curr_size}")
    print(f"  _num_slots = {state._num_slots}")
    print(f"  mask = {state.mask}")
    if hasattr(state, "_batch_bitmap"):
        print(f"  _batch_bitmap = {state._batch_bitmap.value}")
    if hasattr(state, "_batch_accumulation"):
        print(f"  _batch_accumulation = {state._batch_accumulation}")

def print_config(queue):
    cfg = queue.config
    print(f"Queue Config:")
    print(f"  _num_slots = {cfg._num_slots}")
    print(f"  slot_size = {cfg.slot_size}")
    print(f"  queue_type = {cfg.queue_type}")
    print(f"  num_workers = {cfg.num_workers}")

def alloc_testing() -> tuple:
    a = SharedTaskQueue(1,128, 22)
    b = LocalTaskQueue()
    return (a,b)

#alloc has to be successful anyway
def enqueue_testing():
    c, d = alloc_testing()
    for i in range (0,10):
        ArgArray2 = ctypes.c_uint64 * 2
        arg1 = ctypes.c_uint64 * 2
        arg1 = ArgArray2()
        arg1[0] = 1*i
        arg1[1] = 3*i
        MetaArray = ctypes.c_uint8 * 40
        meta = MetaArray()
        c.enqueue(i,i+1,
                arg1,meta)
    return(c,d)
def copy_testing():
    a,b = enqueue_testing ()
    print(a)
    #individual enqueue
    b.enqueue(a.slots[0])
    # batch enqueue
    b.batch_enqueue(110,a.slots[0])
    b.batch_enqueue(250, a.slots[a._state.tail])
    print(b)
    print_config(b)
    print_config(a)
    print_state(b)
    print_state(a)
import time



start = time.perf_counter()
if __name__ == "__main__":
    #alloc_testing()
    #enqueue_testing()

    start = time.perf_counter()
    try:
        copy_testing()
    except (AllocationError, RuntimeError) as e:
        print(f"err{e}")
        pass
    end = time.perf_counter()
    print(f"Elapsed: {end - start:.6f} seconds")
    circular_test_local_queue()