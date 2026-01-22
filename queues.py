import ctypes
from enum import IntEnum
from slot import TaskSlot128
import threading


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
    _batch_list: internal variable to indicate which worker is participating in batching.
    '''
    __slots__ = ("head", "tail", "_num_slots", "curr_size", "mask", "_batch_list")
    def __init__(self, _num_slots: int):
        self.head = 0
        self.tail = 0
        self._num_slots = _num_slots
        #curr_size represents number of occupied slots
        #mostly helps queue limit checking
        self.curr_size = 0
        self.mask = _num_slots - 1

        #each time batch is requested, worker
        #mitigation plan is to have a worker that signals once batch_list ==0
        '''
        self._batch_list : Each time worker batches task, it flips ith bit (i is the determined by internal worker_id) to 1 and 
        mark itself's internal variable, _batch_ref = 1.
        whenever worker tries to batch but its _batch_ref = 1, it returns immediately and do other tasks.
        this is to avoid putting too much burden on a shared producer(scheduling) queue.
        in the future, to mitigate problem with a single shared queue with many workers, scheduler_worker will be introduced.
        However, I recommend to create multiple shared queues with each having a distinct set of dedicated workers.
            For instance:
                Queue1 has {0,1,2,3,4,5,6,7} workers
                Queue2 has {8,...15} workers and so on.
        '''
        self._batch_list = ctypes.c_uint64


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
        
        _num_slots = _num_slots or self.DEFAULT_NUM_SLOTS
        self._config = QueueConfig(
            _num_slots=_num_slots,
            slot_size=ctypes.sizeof(slot),
            queue_type=qtype,
            num_workers = 1
        )
        raw_bytes =_num_slots * self._config.slot_size+ 63
        self._raw_buffer = ctypes.create_string_buffer(raw_bytes)
        raw_addr =ctypes.addressof(self._raw_buffer)
        self._slots_base = (raw_addr + 63)& ~63

        SlotArray =TaskSlot128 *_num_slots
        self.slots =SlotArray.from_address(self._slots_base)

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
        next_tail = (tail +1) &state.mask
        
        available_slots = self._state._num_slots - self._state.curr_size 
        if available_slots < 1:
            raise RuntimeError("Queue full")
        #just copy everything from the given slot
        ctypes.memmove(ctypes.byref(slots[tail]), ctypes.byref(slot),ctypes.sizeof(slot))

        state.tail =next_tail

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

    #this takes count as an argument and copies count * slots at once using memmove.
    # benchmarking needed to figure out the speed
    # Batch_enqueue needs a bit more logic to handle failure
    #   1: flatenning head and tail to check the avaialble slots
    #   2: if count > available slot, we have to readjust the slots.
    #       2: count will be adjusted by the worker calcuating batch
    #           a bit unsafe tho.
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
        ctypes.memmove(ctypes.byref(slots[tail]), ctypes.byref(slot),ctypes.sizeof(slot) * count)
        state.tail =next_tail



    #
    #   UTILS
    #
    def is_empty(self) -> bool:
        s = self._state
        return s.head == s.tail

    def is_full(self) -> bool:
        s = self._state
        return ((s.tail + 1) & s.mask) ==s.head

    def count(self) -> int:
        s = self._state
        return (s.tail - s.head) &s.mask
    
    def __str__(self):
        state = self._state
        info=[
            f"[LOCAL TASK QUEUE]"
            f"_num_slots={self._num_slots}",
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

# ============================================================
# SharedTaskQueue with identical structure style to LocalTaskQueue
# ============================================================
class SharedTaskQueue:
    '''
    queue_id : int
    _num_slots: int
    RIGHT NOW Slot is predifned manually
    '''
    def __init__(self, queue_id : int, _num_slots: int, num_workers: int):

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
        next_tail = (tail +1) &state.mask

        if next_tail == state.head:
            raise RuntimeError(f"[Queue][Shared][ERROR] Queue Id: {self.queue_id}- Queue full")

        slot = slots[tail]

        # write payload (private, not yet published)
        slot.tsk_id = tsk_id
        slot.fn_id  = fn_id

        #I just found memmove and it bypass python typechecking!
        #bench marking has shown that this is like 2~3x faster
        #but it does create a temp slot but its a tiny object and is already aligend soo

        ctypes.memmove(ctypes.byref(slot.args),args,16)
        ctypes.memmove(ctypes.byref(slot.meta),meta,40)

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
            f"_num_slots={self._num_slots}",
            f"head={state.head}, tail={state.tail}, count={self.count()}",
            f"full={self.is_full()}, empty={self.is_empty()}",
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
#FOR TESTING QUEUE
#
#
def alloc_testing() -> tuple:
    a = SharedTaskQueue(1,128, 1)
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
    b.batch_enqueue(310,a.slots[0])
    print(b)
    
import time

start = time.perf_counter()
if __name__ == "__main__":
    #alloc_testing()
    #enqueue_testing()

    start = time.perf_counter()
    try:
        copy_testing()
    except RuntimeError as e:
        print(f"err{e}")
        pass
    end = time.perf_counter()
    print(f"Elapsed: {end - start:.6f} seconds")