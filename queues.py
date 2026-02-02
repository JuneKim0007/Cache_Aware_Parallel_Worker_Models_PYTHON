# REFER TO https://avc-1.gitbook.io/ringqueuebitmapbatchingamongmultipleconsumer/
# Though I was somewhat confident in the design, just like any concurrency modles, I could never be sure so...
# I have simulated this Using various LLM Products out there in the market.
# So far all correctness has been checked with concurrency and paralleism bugs.
# However, there are still needs to check and implement minor bugs like checking for queue empty, or full.
#
import ctypes
from enum import IntEnum
from multiprocessing import Lock
from multiprocessing.shared_memory import SharedMemory
from typing import Type, List, TYPE_CHECKING

from slot import TaskSlot128, SlotVariant, get_slot_variant, SLOT_REGISTRY
from errors import ErrorCode, Component, format_error, validate_num_slots, validate_num_workers

if TYPE_CHECKING:
    from worker import Worker


class AllocationError(Exception):
    pass


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
        ("_num_slots", ctypes.c_uint32),
        ("slot_size", ctypes.c_uint32),
        ("queue_type", ctypes.c_uint32),
        ("num_workers", ctypes.c_uint8)
    ]


#============================================================
# SHARED QUEUE STATE (ctypes for shared memory)
#============================================================
class SharedQueueState(ctypes.Structure):
    '''
    Queue state in shared memory for cross-process access.
    
    head: Consumer dequeue position (consumers advance this during batch claim)
    tail: Producer enqueue position (producer advances this during enqueue)
    _num_slots: Total slots in ring buffer
    mask: For modular arithmetic (_num_slots - 1)
    logical_occupancy: Producer's view of occupied slots
    active_batches: Bitmap tracking which consumers have active batches
    batch_accumulation_counter: Accumulates slots claimed during current batch cycle
    committed_accumulation: Ready for producer to consume
    is_committed: Flag indicating batches completed
    active_worker_count: Current number of active workers
    available_consumer_ids: Bitmap of available consumer IDs (1=available)
    '''
    _fields_ = [
        ("head", ctypes.c_uint32),
        ("tail", ctypes.c_uint32),
        ("_num_slots", ctypes.c_uint32),
        ("mask", ctypes.c_uint32),
        ("logical_occupancy", ctypes.c_uint32),
        ("active_batches", ctypes.c_uint64),
        ("batch_accumulation_counter", ctypes.c_uint32),
        ("committed_accumulation", ctypes.c_uint32),
        ("is_committed", ctypes.c_uint8),
        ("active_worker_count", ctypes.c_uint8),
        ("_pad", ctypes.c_uint8 * 2),
        ("available_consumer_ids", ctypes.c_uint64),
        ("_padding", ctypes.c_uint8 * 16),
    ]

SHARED_QUEUE_STATE_SIZE = ctypes.sizeof(SharedQueueState)


#============================================================
# LOCAL QUEUE STATE (Python object, not shared)
#============================================================
class LocalQueueState:
    '''
    Local queue state (single process, no sharing needed).
    '''
    __slots__ = ("head", "tail", "_num_slots", "mask", "curr_size")
    
    def __init__(self, _num_slots: int):
        self.head: int = 0
        self.tail: int = 0
        self._num_slots: int = _num_slots
        self.mask: int = _num_slots - 1
        self.curr_size: int = 0


#============================================================
# CONSUMER STATE
#============================================================
class ConsumerState:
    '''
    Per-consumer state for batch range-claiming.
    '''
    __slots__ = ("consumer_id", "batch_head", "batch_tail", "batch_size", 
                 "local_buffer", "slot_class")
    
    def __init__(self, consumer_id: int, max_batch_size: int = 0, 
                 slot_class: Type[ctypes.Structure] = TaskSlot128):
        if consumer_id >= 64:
            raise ValueError(format_error(
                ErrorCode.E011_INVALID_CONSUMER_ID,
                Component.WORKER,
                f"consumer_id must be < 64 for bitmap, got {consumer_id}"
            ))
        
        self.consumer_id: int = consumer_id
        self.batch_head: int = 0
        self.batch_tail: int = 0
        self.batch_size: int = 0
        self.slot_class = slot_class
        
        if max_batch_size > 0:
            SlotArray = slot_class * max_batch_size
            self.local_buffer = SlotArray()
        else:
            self.local_buffer = None


#============================================================
# Local Queue (single process, no shared memory)
#============================================================
class LocalTaskQueue:
    '''
    Single-producer, single-consumer queue without batching.
    Used within a single process (worker's local buffer).
    '''
    __slots__ = ("_state", "_slots_base", "slots", "_config", "_raw_buffer", 
                 "_slot_class", "_slot_variant")
    DEFAULT_NUM_SLOTS = 256

    def __init__(self, 
                 _num_slots: int | None = None,
                 slot_class: Type[ctypes.Structure] | None = None,
                 qtype: ProcQueueTypes = ProcQueueTypes.TASK_DISPATCH,
                 shared_queue: 'SharedTaskQueue | None' = None):
        '''
        Initialize local queue.
        
        Args:
            _num_slots: Number of slots (default: 256)
            slot_class: ctypes.Structure class (default: TaskSlot128)
            qtype: Queue type enum
            shared_queue: Optional SharedTaskQueue to derive config from
        '''
        if shared_queue is not None:
            slot_class = slot_class or shared_queue.slot_class
            _num_slots = _num_slots or self.DEFAULT_NUM_SLOTS
        else:
            slot_class = slot_class or TaskSlot128
            _num_slots = _num_slots or self.DEFAULT_NUM_SLOTS
        
        self._slot_class = slot_class
        self._slot_variant = get_slot_variant(slot_class)
        
        self._config = QueueConfig(
            _num_slots=_num_slots,
            slot_size=ctypes.sizeof(slot_class),
            queue_type=qtype,
            num_workers=1
        )
        
        #64-byte aligned buffer
        raw_bytes = _num_slots * self._config.slot_size + 63
        self._raw_buffer = ctypes.create_string_buffer(raw_bytes)
        raw_addr = ctypes.addressof(self._raw_buffer)
        self._slots_base = (raw_addr + 63) & ~63

        SlotArray = slot_class * _num_slots
        self.slots = SlotArray.from_address(self._slots_base)
        self._state = LocalQueueState(_num_slots)

    @property
    def config(self) -> QueueConfig:
        return self._config
    
    @property
    def slot_class(self) -> Type[ctypes.Structure]:
        return self._slot_class
    
    @property
    def slot_variant(self) -> SlotVariant:
        return self._slot_variant

    def enqueue(self, slot):
        state = self._state
        tail = state.tail
        
        if state.curr_size == state._num_slots:
            raise RuntimeError(format_error(
                ErrorCode.E001_QUEUE_FULL, Component.LOCAL_QUEUE, "Queue full"
            ))
        
        ctypes.memmove(ctypes.byref(self.slots[tail]), ctypes.byref(slot), ctypes.sizeof(slot))
        state.tail = (tail + 1) & state.mask
        state.curr_size += 1

    def dequeue(self) -> int:
        state = self._state
        head = state.head
        
        if state.curr_size == 0:
            raise RuntimeError(format_error(
                ErrorCode.E005_QUEUE_EMPTY, Component.LOCAL_QUEUE, "Queue empty"
            ))

        tsk_id = self.slots[head].tsk_id
        state.head = (head + 1) & state.mask
        state.curr_size -= 1
        return tsk_id

    def batch_enqueue(self, count, slot):
        '''Batch enqueue for local queue'''
        state = self._state
        tail = state.tail

        available_slots = state._num_slots - state.curr_size 
        
        if available_slots < count:
            raise RuntimeError(format_error(
                ErrorCode.E001_QUEUE_FULL, Component.LOCAL_QUEUE, 
                f"Not enough space: need {count}, have {available_slots}"
            ))
        
        #Handle wrap-around
        first = min(count, state._num_slots - tail)
        second = count - first

        ctypes.memmove(
            ctypes.byref(self.slots[tail]),
            ctypes.byref(slot),
            ctypes.sizeof(slot) * first
        )

        if second > 0:
            ctypes.memmove(
                ctypes.byref(self.slots[0]),
                ctypes.byref(slot, ctypes.sizeof(slot) * first),
                ctypes.sizeof(slot) * second
            )

        state.tail = (tail + count) & state.mask
        state.curr_size += count

    def is_empty(self) -> bool:
        return self._state.curr_size == 0

    def is_full(self) -> bool:
        return self._state.curr_size >= self._state._num_slots

    def count(self) -> int:
        return self._state.curr_size


#============================================================
# SharedTaskQueue (multiprocessing, shared memory)
#============================================================
class SharedTaskQueue:
    '''
    Single-producer, multiple-consumer shared queue with batch range-claiming.
    Uses multiprocessing.Lock and SharedMemory for cross-process access.
    '''
    __slots__ = (
        "queue_id", "_state", "_slots_base", "slots", "_config",
        "_shm_slots", "_shm_state", "lock", "batch_commit_lock",
        "_slot_class", "_slot_variant", "queue_name", "_queue_type",
        "_log_queue", "shm_slots_name", "shm_state_name"
    )
    
    DEFAULT_NUM_SLOTS = 256
    DEFAULT_QUEUE_NAME = "shared_queue"
    
    def __init__(self, 
                 queue_id: int,
                 _num_slots: int | None = None,
                 num_workers: int = 1,
                 slot_class: Type[ctypes.Structure] = TaskSlot128,
                 queue_name: str | None = None,
                 queue_type: ProcQueueTypes = ProcQueueTypes.TASK_DISTRIBUTE,
                 lock: Lock = None,
                 batch_lock: Lock = None,
                 log_queue = None):
        '''
        Initialize shared queue.
        
        Args:
            queue_id: Unique identifier
            _num_slots: Number of slots (default: 256, must be power of 2)
            num_workers: Max workers (must be <= 64 for bitmap)
            slot_class: ctypes.Structure class for slots
            queue_name: String identifier
            queue_type: Queue type enum
            lock: multiprocessing.Lock for main operations
            batch_lock: multiprocessing.Lock for batch commit
            log_queue: multiprocessing.Queue for error logging
        '''
        _num_slots = _num_slots or self.DEFAULT_NUM_SLOTS
        
        #Validate parameters
        err = validate_num_slots(_num_slots)
        if err:
            raise ValueError(format_error(ErrorCode.E002_INVALID_NUM_SLOTS, Component.SHARED_QUEUE, err))
        
        err = validate_num_workers(num_workers)
        if err:
            raise AllocationError(format_error(ErrorCode.E003_NUM_WORKERS_EXCEEDED, Component.SHARED_QUEUE, err))
        
        self.queue_id = queue_id
        self.queue_name = queue_name or self.DEFAULT_QUEUE_NAME
        self._slot_class = slot_class
        self._slot_variant = get_slot_variant(slot_class)
        self._queue_type = queue_type
        self._log_queue = log_queue
        
        slot_size = ctypes.sizeof(slot_class)
        
        self._config = QueueConfig(
            _num_slots=_num_slots,
            slot_size=slot_size,
            queue_type=queue_type,
            num_workers=num_workers
        )
        
        #Shared memory for slots (64-byte aligned)
        slot_buffer_size = _num_slots * slot_size
        self._shm_slots = SharedMemory(create=True, size=slot_buffer_size)
        self.shm_slots_name = self._shm_slots.name
        
        SlotArray = slot_class * _num_slots
        self.slots = SlotArray.from_buffer(self._shm_slots.buf)
        
        #Shared memory for queue state
        self._shm_state = SharedMemory(create=True, size=SHARED_QUEUE_STATE_SIZE)
        self.shm_state_name = self._shm_state.name
        
        self._state = SharedQueueState.from_buffer(self._shm_state.buf)
        self._state._num_slots = _num_slots
        self._state.mask = _num_slots - 1
        self._state.head = 0
        self._state.tail = 0
        self._state.logical_occupancy = 0
        self._state.active_batches = 0
        self._state.batch_accumulation_counter = 0
        self._state.committed_accumulation = 0
        self._state.is_committed = 0
        self._state.active_worker_count = 0
        self._state.available_consumer_ids = (1 << 64) - 1  #All available
        
        #Multiprocessing locks
        self.lock = lock or Lock()
        self.batch_commit_lock = batch_lock or Lock()

    @property
    def _num_slots(self) -> int:
        return self._config._num_slots
    
    @property
    def config(self) -> QueueConfig:
        return self._config
    
    @property
    def slot_class(self) -> Type[ctypes.Structure]:
        return self._slot_class
    
    @property
    def slot_variant(self) -> SlotVariant:
        return self._slot_variant
    
    @property
    def slot_size(self) -> int:
        return self._config.slot_size

    #
    # PRODUCER OPERATIONS
    #
    def _try_enqueue(self, slot) -> bool:
        '''Try to enqueue once. Returns True on success.'''
        state = self._state
        
        if state.is_committed:
            with self.batch_commit_lock:
                state.logical_occupancy -= state.committed_accumulation
                state.committed_accumulation = 0
                state.is_committed = 0
        
        if state.logical_occupancy >= self._num_slots:
            return False
        
        tail = state.tail
        ctypes.memmove(
            ctypes.byref(self.slots[tail]),
            ctypes.byref(slot),
            self.slot_size
        )
        
        state.tail = (tail + 1) & state.mask
        state.logical_occupancy += 1
        return True

    def enqueue(self, slot) -> bool:
        '''
        Enqueue a task slot (non-blocking).
        Returns True on success, False if queue full.
        '''
        success = self._try_enqueue(slot)
        if not success and self._log_queue:
            try:
                self._log_queue.put((-1, format_error(
                    ErrorCode.E001_QUEUE_FULL, Component.SHARED_QUEUE,
                    f"Queue {self.queue_id} full, task dropped"
                )))
            except:
                pass
        return success

    def enqueue_blocking(self, slot, max_wait: float = 10.0) -> bool:
        '''
        Enqueue with blocking (exponential backoff).
        
        Args:
            slot: Task slot to enqueue
            max_wait: Maximum wait time in seconds
            
        Returns:
            True if enqueued, False if timeout
        '''
        import time
        
        backoff = 0.001
        backoff_cap = 0.1
        elapsed = 0.0
        
        while elapsed < max_wait:
            if self._try_enqueue(slot):
                return True
            
            time.sleep(backoff)
            elapsed += backoff
            backoff = min(backoff * 2, backoff_cap)
        
        if self._log_queue:
            try:
                self._log_queue.put((-1, format_error(
                    ErrorCode.E001_QUEUE_FULL, Component.SHARED_QUEUE,
                    f"Queue {self.queue_id} full after {max_wait}s timeout"
                )))
            except:
                pass
        return False

    #
    # CONSUMER OPERATIONS
    #
    def claim_batch_range(self, consumer: ConsumerState, batch_size: int) -> bool:
        '''
        Consumer claims a batch range atomically.
        Returns True if claim succeeded, False otherwise.
        '''
        state = self._state
        
        max_allowed_batch = self._num_slots // 2
        if batch_size > max_allowed_batch:
            raise ValueError(format_error(
                ErrorCode.E004_INVALID_BATCH_SIZE, Component.SHARED_QUEUE,
                f"batch_size ({batch_size}) exceeds queue_size//2 ({max_allowed_batch})"
            ))
        
        claim_succeeded = False
        with self.lock:
            bit_position = 1 << consumer.consumer_id
            state.active_batches |= bit_position
            
            available = (state.tail - state.head) & state.mask
            if available >= batch_size:
                consumer.batch_head = state.head
                consumer.batch_tail = (state.head + batch_size) & state.mask
                consumer.batch_size = batch_size
                
                state.head = consumer.batch_tail
                state.batch_accumulation_counter += batch_size
                
                claim_succeeded = True
            else:
                state.active_batches &= ~bit_position
        
        if not claim_succeeded:
            consumer.batch_size = 0
            return False
        
        return True

    def dequeue_batch(self, consumer: ConsumerState) -> bool:
        '''Copy claimed batch to consumer's local buffer.'''
        if consumer.batch_size == 0:
            return False
        
        if consumer.local_buffer is not None:
            self._copy_batch_to_local(consumer)
        
        return True

    def finish_batch(self, consumer: ConsumerState):
        '''Consumer finishes batch and coordinates with producer.'''
        state = self._state
        bit_position = 1 << consumer.consumer_id
        
        bitmap_became_zero = False
        with self.lock:
            state.active_batches &= ~bit_position
            bitmap_became_zero = (state.active_batches == 0)
        
        if bitmap_became_zero:
            with self.batch_commit_lock:
                state.committed_accumulation += state.batch_accumulation_counter
                state.batch_accumulation_counter = 0
                state.is_committed = 1
        
        consumer.batch_head = 0
        consumer.batch_tail = 0
        consumer.batch_size = 0

    def _copy_batch_to_local(self, consumer: ConsumerState):
        if consumer.local_buffer is None:
            return
            
        state = self._state
        batch_head = consumer.batch_head
        batch_size = consumer.batch_size
        slot_size = ctypes.sizeof(consumer.slot_class)
        
        if (batch_head + batch_size) <= state._num_slots:
            ctypes.memmove(
                ctypes.byref(consumer.local_buffer[0]),
                ctypes.byref(self.slots[batch_head]),
                slot_size * batch_size
            )
        else:
            first_part = state._num_slots - batch_head
            second_part = batch_size - first_part
            
            ctypes.memmove(
                ctypes.byref(consumer.local_buffer[0]),
                ctypes.byref(self.slots[batch_head]),
                slot_size * first_part
            )
            
            ctypes.memmove(
                ctypes.byref(consumer.local_buffer[first_part]),
                ctypes.byref(self.slots[0]),
                slot_size * second_part
            )

    #
    # WORKER MANAGEMENT
    #
    def allocate_consumer_id(self) -> int:
        state = self._state
        with self.lock:
            if state.available_consumer_ids == 0:
                return -1
            
            avail = state.available_consumer_ids
            consumer_id = (avail & -avail).bit_length() - 1
            
            state.available_consumer_ids &= ~(1 << consumer_id)
            state.active_worker_count += 1
            
            return consumer_id
    
    def release_consumer_id(self, consumer_id: int):
        '''Release a consumer ID back to available pool.'''
        if consumer_id < 0 or consumer_id >= 64:
            return
        
        state = self._state
        with self.lock:
            state.available_consumer_ids |= (1 << consumer_id)
            if state.active_worker_count > 0:
                state.active_worker_count -= 1

    #
    # STATUS
    #
    def is_empty(self) -> bool:
        return self._state.logical_occupancy == 0
    
    def get_available_slots(self) -> int:
        return self._num_slots - self._state.logical_occupancy
    
    def get_actual_occupancy(self) -> int:
        state = self._state
        return (state.tail - state.head) & state.mask
    
    def get_active_worker_count(self) -> int:
        return self._state.active_worker_count

    def cleanup(self):
        '''Release shared memory (call from parent process)'''
        try:
            self._shm_slots.close()
            self._shm_slots.unlink()
        except:
            pass
        try:
            self._shm_state.close()
            self._shm_state.unlink()
        except:
            pass
    
    def __str__(self):
        state = self._state
        actual_occ = self.get_actual_occupancy()
        
        info = [
            f"[SharedTaskQueue] queue_id={self.queue_id}",
            f"queue_name={self.queue_name}",
            f"slot_class={self._slot_class.__name__}",
            f"slot_variant={self._slot_variant.name}",
            f"slot_size={self.slot_size}",
            f"queue_type={self._queue_type.name}",
            f"_num_slots={self._num_slots}",
            f"head={state.head}, tail={state.tail}",
            f"logical_occupancy={state.logical_occupancy}",
            f"actual_occupancy={actual_occ}",
            f"active_batches=0x{state.active_batches:016x}",
            f"batch_accumulation={state.batch_accumulation_counter}",
            f"committed_accumulation={state.committed_accumulation}",
            f"is_committed={state.is_committed}",
            f"active_worker_count={state.active_worker_count}",
        ]

        tasks = []
        for i in range(min(10, actual_occ)):
            idx = (state.head + i) & state.mask
            slot = self.slots[idx]
            tasks.append(f"idx={idx}: tsk_id={slot.tsk_id}, fn_id={slot.fn_id}")
        info.extend(tasks)
        
        return "\n".join(info)