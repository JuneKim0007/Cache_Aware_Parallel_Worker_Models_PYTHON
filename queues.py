# Based on complete clarifications from the paper author, me!!!
# REFER TO https://avc-1.gitbook.io/ringqueuebitmapbatchingamongmultipleconsumer/
# Though I was somewhat confident in the design, just like any concurrency modles, you can never be sure so:
# I have simulated this Using various LLM Products out there in the market.
# So far all correctness has been checked with concurrency and paralleism bugs.
# However, there are still needs to check and implement minor bugs like checking for queue empty, or full.
import ctypes
from enum import IntEnum
from slot import TaskSlot128
import threading

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
# QUEUE STATE 
#============================================================
class QueueState:
    '''
    Ring queue state matching paper's design:
    
    head: Consumer dequeue position (consumers advance this during batch claim)
    tail: Producer enqueue position (producer advances this during enqueue)
    _num_slots: Total slots in ring buffer
    mask: For modular arithmetic (_num_slots - 1)
    
    --For local queue only:
    curr_size: Number of occupied slots
    
    --For shared queue - Coordination layer:
    logical_occupancy: Producer's view of occupied slots (updated on enqueue success and batch commit)
    active_batches: Bitmap tracking which consumers have active batches
    batch_accumulation_counter: Accumulates slots claimed during current batch cycle
    committed_accumulation: Ready for producer to consume (decrement logical_occupancy)
    is_committed: Flag indicating batches completed and producer should update logical_occupancy
    num_batch_participants: Mechanism 3 - limits concurrent batchers
    '''
    __slots__ = (
        "head",
        "tail", 
        "_num_slots",
        "mask",
        "curr_size",
        "logical_occupancy",
        "active_batches",
        "batch_accumulation_counter",
        "committed_accumulation",
        "is_committed",
        "num_batch_participants"
    )
    
    def __init__(self, _num_slots: int, is_shared: bool = False):
        self.head: int = 0
        self.tail: int = 0
        self._num_slots: int = _num_slots
        self.mask: int = _num_slots - 1
        
        #Local queue only
        self.curr_size: int = 0
        
        #Shared queue only - coordination layer
        if is_shared:
            self.logical_occupancy: int = 0
            self.active_batches = ctypes.c_uint64(0)
            self.batch_accumulation_counter: int = 0
            self.committed_accumulation: int = 0
            self.is_committed: bool = False
            self.num_batch_participants: int = 0


#============================================================
# CONSUMER STATE
#============================================================
class ConsumerState:
    '''
    Per-consumer state for batch range-claiming:
    
    consumer_id: Unique ID (0-63) for bitmap indexing
    batch_head: Start index of claimed batch range
    batch_tail: End index of claimed batch range
    batch_size: Number of slots in current batch
    local_buffer: Optional local storage for fetched slots
    

    '''
    __slots__ = ("consumer_id", "batch_head", "batch_tail", "batch_size", "local_buffer")
    
    def __init__(self, consumer_id: int, max_batch_size: int = 0):
        if consumer_id >= 64:
            raise ValueError(f"consumer_id must be < 64 for bitmap, got {consumer_id}")
        
        self.consumer_id: int = consumer_id
        self.batch_head: int = 0
        self.batch_tail: int = 0
        self.batch_size: int = 0
        
        if max_batch_size > 0:
            SlotArray = TaskSlot128 * max_batch_size
            self.local_buffer = SlotArray()
        else:
            self.local_buffer = None


# ============================================================
# Local Queue
# ============================================================
class LocalTaskQueue:
    '''Single-producer, single-consumer queue without batching.'''
    __slots__ = ("_state", "_slots_base", "slots", "_config", "_raw_buffer")
    DEFAULT_NUM_SLOTS = 256

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
        
        raw_bytes = _num_slots * self._config.slot_size + 63
        self._raw_buffer = ctypes.create_string_buffer(raw_bytes)
        raw_addr = ctypes.addressof(self._raw_buffer)
        self._slots_base = (raw_addr + 63) & ~63

        SlotArray = TaskSlot128 * _num_slots
        self.slots = SlotArray.from_address(self._slots_base)
        self._state = QueueState(_num_slots, is_shared=False)

        print(f"[QUEUE][LOCAL][INITIALIZED] onto Worker 'workerID' on 'TS'")

    @property
    def config(self) -> QueueConfig:
        return self._config

    def enqueue(self, slot):
        state = self._state
        slots = self.slots
        tail = state.tail
        next_tail = (tail + 1) & state.mask
        
        if state.curr_size == state._num_slots:
            raise RuntimeError("Queue full")
        
        ctypes.memmove(ctypes.byref(slots[tail]), ctypes.byref(slot), ctypes.sizeof(slot))
        self._state.tail = next_tail
        self._state.curr_size += 1

    def dequeue(self) -> int:
        state = self._state
        slots = self.slots
        head = state.head
        
        if state.curr_size == 0:
            raise RuntimeError("Queue empty")

        tsk_id = slots[head].tsk_id
        self._state.head = (head + 1) & state.mask
        self._state.curr_size -= 1
        return tsk_id

    def batch_enqueue(self, count, slot):
        '''Batch enqueue for local queue'''
        state = self._state
        slots = self.slots
        tail = state.tail
        next_tail = (tail + count) & state.mask

        available_slots = self._state._num_slots - self._state.curr_size 
        
        if available_slots < count:
            raise RuntimeError(f"[ERROR][Queue][Local] onto Worker 'workerID' - Queue full")
        
        #Handle wrap-around
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

        self._state.tail = next_tail
        self._state.curr_size += count

    def is_empty(self) -> bool:
        return self._state.head == self._state.tail

    def is_full(self) -> bool:
        return self._state._num_slots - self._state.curr_size < 1

    def count(self) -> int:
        return self._state.curr_size


#============================================================
# SharedTaskQueue  
#============================================================
class SharedTaskQueue:
    '''
    Single-producer, multiple-consumer shared queue with batch range-claiming.
    '''
    __slots__ = (
        "queue_id", "_state", "_slots_base", "slots", "_config",
        "_raw_buffer", "lock", "batch_commit_lock", "_max_batch_participants"
    )
    
    DEFAULT_MAX_BATCH_PARTICIPANTS = 8
    
    def __init__(self, queue_id: int, _num_slots: int, num_workers: int,
                 max_batch_participants: int | None = None):
        
        if num_workers > 64:
            raise AllocationError(f"num_workers > 64, got {num_workers}")

        self._config = QueueConfig(
            _num_slots=_num_slots,
            slot_size=ctypes.sizeof(TaskSlot128),
            queue_type=ProcQueueTypes.TASK_DISTRIBUTE,
            num_workers=num_workers
        )

        self.queue_id = queue_id
        
        #64-byte aligned memory layout
        raw_bytes = _num_slots * self._config.slot_size + 63
        self._raw_buffer = ctypes.create_string_buffer(raw_bytes)
        raw_addr = ctypes.addressof(self._raw_buffer)
        self._slots_base = (raw_addr + 63) & ~63

        SlotArray = TaskSlot128 * _num_slots
        self.slots = SlotArray.from_address(self._slots_base)
        self._state = QueueState(_num_slots, is_shared=True)
        
        #Two-lock design
        self.lock = threading.Lock()
        self.batch_commit_lock = threading.Lock()
        self._max_batch_participants = max_batch_participants or self.DEFAULT_MAX_BATCH_PARTICIPANTS
        
        print(f"[QUEUE][SHARED][INITIALIZED] queue_id={self.queue_id}")

    @property
    def _num_slots(self) -> int:
        return self._config._num_slots
    
    @property
    def config(self) -> QueueConfig:
        return self._config

    #
    # PRODUCER OPERATIONS
    #
    def enqueue(self, slot):
        state = self._state
    
        if state.is_committed:
            with self.batch_commit_lock:
                state.logical_occupancy -= state.committed_accumulation
                state.committed_accumulation = 0
                state.is_committed = False
        
        if state.logical_occupancy >= self._num_slots:
            raise RuntimeError(f"[Queue][Shared][ERROR] Queue Id: {self.queue_id} - Queue full")
        
        #Write at TAIL position (producer enqueue position)
        tail = state.tail
        ctypes.memmove(
            ctypes.byref(self.slots[tail]),
            ctypes.byref(slot),
            ctypes.sizeof(slot)
        )
        
        state.tail = (tail + 1) & state.mask
        state.logical_occupancy += 1

    #
    # CONSUMER OPERATIONS
    #
    def claim_batch_range(self, consumer: ConsumerState, batch_size: int) -> bool:
        '''
        Mechanism 1 & 3: Consumer claims a batch range atomically.
        Returns True if claim succeeded, False otherwise.
        '''
        state = self._state
        

        max_allowed_batch = self._num_slots // 2
        if batch_size > max_allowed_batch:
            raise ValueError(
                f"batch_size ({batch_size}) exceeds hard limit of queue_size//2 "
                f"({max_allowed_batch}). Maximum batch_size is {max_allowed_batch}."
            )
        # check the limit before the lock
        if state.num_batch_participants >= self._max_batch_participants:
            return False
        
        #Acquire lock for atomic bitmap set + range marking
        claim_succeeded = False
        with self.lock:
            bit_position = 1 << consumer.consumer_id
            state.active_batches.value |= bit_position
            
            #Recheck for avoding race condition
            if state.num_batch_participants >= self._max_batch_participants:
                state.active_batches.value &= ~bit_position
            else:
                #Check available slots for claiming
                #Occupied region is [head, tail) in circular buffer
                #Available to claim = (tail - head) % N
                available = (state.tail - state.head) & state.mask
                if available >= batch_size:
                    state.num_batch_participants += 1
                    
                    #Record batch range;consumer claims from HEAD
                    consumer.batch_head = state.head
                    consumer.batch_tail = (state.head + batch_size) & state.mask
                    consumer.batch_size = batch_size
                    
                    #Advance HEAD to mark claimed range
                    state.head = consumer.batch_tail
                    
                    #Update batch accumulation counter
                    state.batch_accumulation_counter += batch_size
                    
                    claim_succeeded = True
                else:
                    state.active_batches.value &= ~bit_position
        
        #Lock released if claim succeeded, consumer can now dequeue outside critical section
        #If failed, bitmap already cleared inside lock
        if not claim_succeeded:
            consumer.batch_size = 0
            return False
        
        return True

    def dequeue_batch(self, consumer: ConsumerState) -> bool:
        '''
        Mechanism 2: Dequeue claimed batch (outside critical section).
        
        Copies slots from shared queue to consumer's local buffer.
        '''
        if consumer.batch_size == 0:
            return False
        
        if consumer.local_buffer is not None:
            self._copy_batch_to_local(consumer)
        
        return True

    def finish_batch(self, consumer: ConsumerState):
        '''
        Mechanism 2: Consumer finishes batch and coordinates with producer.
        
        CRITICAL: This is called AFTER consumer has dequeued and processed tasks.
        Bitmap clearing happens here (AFTER dequeue) to prevent producer from
        overwriting slots while consumer is still processing them.
        
        Steps:
        1. Clear consumer's bit in bitmap (AFTER dequeue complete)
        2. If bitmap becomes zero (all batches done):
           - Acquire batch_commit_lock
           - Update committed_accumulation
           - Reset batch_accumulation_counter
           - Reset num_batch_participants
           - Set is_committed = true
           - Release lock
        '''
        state = self._state
        bit_position = 1 << consumer.consumer_id
        
        #Clear bitmap AFTER dequeue is complete
        #This prevents producer from overwriting slots still being processed
        bitmap_became_zero = False
        with self.lock:
            state.active_batches.value &= ~bit_position
            bitmap_became_zero = (state.active_batches.value == 0)
        
        #If all batches complete, commit to producer
        if bitmap_became_zero:
            with self.batch_commit_lock:
                #Aggregate dequeued slots
                state.committed_accumulation += state.batch_accumulation_counter
                state.batch_accumulation_counter = 0
                
                state.num_batch_participants = 0
                
                #Signal producer that committed_accumulation is ready
                state.is_committed = True
        
        #Reset consumer's batch state
        consumer.batch_head = 0
        consumer.batch_tail = 0
        consumer.batch_size = 0

    def _copy_batch_to_local(self, consumer: ConsumerState):
        '''Helper to copy batch from shared queue to consumer's local buffer'''
        if consumer.local_buffer is None:
            return
            
        state = self._state
        batch_head = consumer.batch_head
        batch_size = consumer.batch_size
        
        #Handle wrap-around in circular buffer
        if (batch_head + batch_size) <= state._num_slots:
            #No wrap-around
            ctypes.memmove(
                ctypes.byref(consumer.local_buffer[0]),
                ctypes.byref(self.slots[batch_head]),
                ctypes.sizeof(TaskSlot128) * batch_size
            )
        else:
            #Wrap-around: copy in two parts
            first_part = state._num_slots - batch_head
            second_part = batch_size - first_part
            
            ctypes.memmove(
                ctypes.byref(consumer.local_buffer[0]),
                ctypes.byref(self.slots[batch_head]),
                ctypes.sizeof(TaskSlot128) * first_part
            )
            
            ctypes.memmove(
                ctypes.byref(consumer.local_buffer[first_part]),
                ctypes.byref(self.slots[0]),
                ctypes.sizeof(TaskSlot128) * second_part
            )

    #
    # WARNING: No legacy single-item dequeue provided
    #
    # The Shared Queue is designed for batch operations only. Single-item dequeue
    # would not coordinate properly with the batch range-claiming mechanism:
    # Because ...
    #   It would advance head without updating batch_accumulation_counter
    #   It wouldn't participate in bitmap coordination
    #   It could break the producer's logical_occupancy tracking
    # For this SharedTaskQueue, always use the batch workflow; 
    # even for single fetching.

    def is_empty(self) -> bool:
        '''Check if queue is empty from producer perspective'''
        return self._state.logical_occupancy == 0
    
    def get_available_slots(self) -> int:
        '''Get available slots from producer perspective'''
        return self._num_slots - self._state.logical_occupancy
    
    def get_actual_occupancy(self) -> int:
        '''Get actual occupancy based on head/tail pointers'''
        state = self._state
        return (state.tail - state.head) & state.mask
    
    def __str__(self):
        state = self._state
        actual_occ = self.get_actual_occupancy()
        
        info = [
            f"[SharedTaskQueue] queue_id={self.queue_id}",
            f"_num_slots={self._num_slots}",
            f"head={state.head}, tail={state.tail}",
            f"logical_occupancy={state.logical_occupancy} (producer view)",
            f"actual_occupancy={actual_occ} (head-tail)",
            f"fake_occupied={state.logical_occupancy - actual_occ}",
            f"active_batches=0x{state.active_batches.value:016x}",
            f"batch_accumulation={state.batch_accumulation_counter}",
            f"committed_accumulation={state.committed_accumulation}",
            f"is_committed={state.is_committed}",
            f"num_batch_participants={state.num_batch_participants}/{self._max_batch_participants}",
        ]

        tasks = []
        for i in range(min(10, actual_occ)):
            idx = (state.head + i) & state.mask
            slot = self.slots[idx]
            tasks.append(f"idx={idx}: tsk_id={slot.tsk_id}, fn_id={slot.fn_id}")
        info.extend(tasks)
        return "\n".join(info)

