# ============================================================
# QUEUES.PY
# ============================================================
# Queue implementations for multiprocessing task system.
# Contains SharedTaskQueue, LocalTaskQueue, and related structures.
# ============================================================

import os
import sys
import ctypes
import time
from multiprocessing import Lock, Queue
from multiprocessing.shared_memory import SharedMemory
from typing import Type, Optional

from slot import TaskSlot128
from errors import ErrorCode, Component, format_error


#============================================================
# SHARED QUEUE STATE (ctypes structure for shared memory)
#============================================================
class SharedQueueState(ctypes.Structure):
    '''
    Queue state stored in shared memory for cross-process access.
    
    Fields:
    - head/tail: Ring buffer pointers
    - logical_occupancy: Producer's view of occupied slots
    - active_batches: Bitmap of consumers with active batches
    - batch_accumulation_counter: Slots claimed in current batch cycle
    - committed_accumulation: Ready for producer to reclaim
    - is_committed: Flag indicating batches completed
    - active_worker_count: Current number of active workers
    - available_consumer_ids: Bitmap of available consumer IDs
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
        ("num_batch_participants", ctypes.c_uint8),
        ("active_worker_count", ctypes.c_uint8),
        ("_pad1", ctypes.c_uint8),
        ("available_consumer_ids", ctypes.c_uint64),
        ("_padding", ctypes.c_uint8 * 12),
    ]

SHARED_QUEUE_STATE_SIZE = ctypes.sizeof(SharedQueueState)


#============================================================
# LOCAL QUEUE STATE (Python class, not shared)
#============================================================
class LocalQueueState:
    '''
    State for local (single-process) queue.
    '''
    __slots__ = ("head", "tail", "_num_slots", "mask", "curr_size")
    
    def __init__(self, _num_slots: int):
        self.head = 0
        self.tail = 0
        self._num_slots = _num_slots
        self.mask = _num_slots - 1
        self.curr_size = 0


#============================================================
# SHARED TASK QUEUE (Multiprocessing)
#============================================================
class SharedTaskQueue:
    '''
    Single-producer, multiple-consumer shared queue for multiprocessing.
    Uses shared memory for cross-process access.
    
    Key features:
    - Batch range-claiming for efficient dequeue
    - Bitmap coordination for concurrent consumers
    - Exponential backoff for blocking enqueue
    - Consumer ID allocation for dynamic workers
    '''
    DEFAULT_NUM_SLOTS = 256
    
    def __init__(self,
                 queue_id: int,
                 _num_slots: int = 256,
                 num_workers: int = 1,
                 slot_class: Type[ctypes.Structure] = TaskSlot128,
                 queue_name: str = "shared_queue",
                 lock: Lock = None,
                 batch_lock: Lock = None,
                 log_queue: Queue = None):
        
        self.queue_id = queue_id
        self.queue_name = queue_name
        self._slot_class = slot_class
        self._num_slots = _num_slots
        self._slot_size = ctypes.sizeof(slot_class)
        self._log_queue = log_queue
        
        #Shared memory for slots
        slot_buffer_size = _num_slots * self._slot_size
        self._shm_slots = SharedMemory(create=True, size=slot_buffer_size)
        
        #Create slot array from shared memory
        SlotArray = slot_class * _num_slots
        self.slots = SlotArray.from_buffer(self._shm_slots.buf)
        
        #Shared memory for queue state
        self._shm_state = SharedMemory(create=True, size=SHARED_QUEUE_STATE_SIZE)
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
        self._state.num_batch_participants = 0
        self._state.active_worker_count = 0
        self._state.available_consumer_ids = 0
        
        #Multiprocessing locks
        self.lock = lock or Lock()
        self.batch_commit_lock = batch_lock or Lock()
        
        #Store shared memory names for child processes
        self.shm_slots_name = self._shm_slots.name
        self.shm_state_name = self._shm_state.name
    
    @property
    def slot_class(self) -> Type[ctypes.Structure]:
        return self._slot_class
    
    @property
    def slot_size(self) -> int:
        return self._slot_size
    
    #
    # PRODUCER OPERATIONS
    #
    def _try_enqueue(self, slot) -> bool:
        '''
        Try to enqueue once. Returns True on success, False if full.
        '''
        state = self._state
        
        #Reclaim committed slots if available
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
            self._slot_size
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
            err = format_error(
                ErrorCode.E001_QUEUE_FULL,
                Component.SHARED_QUEUE,
                f"Queue {self.queue_id} full, task dropped"
            )
            try:
                self._log_queue.put((-1, err))
            except:
                pass
        return success

    def enqueue_blocking(self, slot, max_wait: float = 10.0) -> bool:
        '''
        Enqueue with blocking (exponential backoff).
        
        Strategy: Start with 1ms sleep, double each retry up to 100ms cap.
        
        Args:
            slot: Task slot to enqueue
            max_wait: Maximum wait time in seconds (default: 10s)
            
        Returns:
            True if enqueued, False if timeout
        '''
        backoff = 0.001  #1ms
        backoff_cap = 0.1  #100ms
        elapsed = 0.0
        
        while elapsed < max_wait:
            if self._try_enqueue(slot):
                return True
            
            time.sleep(backoff)
            elapsed += backoff
            backoff = min(backoff * 2, backoff_cap)
        
        #Timeout
        if self._log_queue:
            err = format_error(
                ErrorCode.E001_QUEUE_FULL,
                Component.SHARED_QUEUE,
                f"Queue {self.queue_id} full after {max_wait}s timeout"
            )
            try:
                self._log_queue.put((-1, err))
            except:
                pass
        return False

    #
    # STATUS OPERATIONS
    #
    def get_actual_occupancy(self) -> int:
        '''Get actual number of slots with data (tail - head).'''
        state = self._state
        return (state.tail - state.head) & state.mask
    
    def get_active_worker_count(self) -> int:
        '''Get current number of active workers.'''
        return self._state.active_worker_count
    
    def is_empty(self) -> bool:
        '''Check if queue is empty.'''
        return self._state.logical_occupancy == 0

    #
    # CONSUMER ID MANAGEMENT
    #
    def allocate_consumer_id(self) -> int:
        '''
        Allocate a consumer ID from available pool.
        Returns consumer_id (0-63) or -1 if none available.
        '''
        state = self._state
        with self.lock:
            if state.available_consumer_ids == 0:
                return -1
            
            #Find first available bit
            avail = state.available_consumer_ids
            consumer_id = (avail & -avail).bit_length() - 1
            
            #Mark as used
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
    # CLEANUP
    #
    def cleanup(self):
        '''
        Cleanup shared memory.
        Only unlinks - does not close (to avoid BufferError).
        '''
        shm_list = [self._shm_slots, self._shm_state]
        
        for shm in shm_list:
            try:
                shm.unlink()
            except FileNotFoundError:
                pass
            except:
                pass
        
        #Prevent __del__ from trying to close
        for shm in shm_list:
            shm._mmap = None
            shm._buf = None
            shm._name = None


#============================================================
# LOCAL TASK QUEUE (Single Process)
#============================================================
class LocalTaskQueue:
    '''
    Single-producer, single-consumer queue for worker-local use.
    Supports efficient batch copy from shared queue.
    '''
    DEFAULT_NUM_SLOTS = 256

    def __init__(self, _num_slots: int = None, slot_class = TaskSlot128):
        _num_slots = _num_slots or self.DEFAULT_NUM_SLOTS
        
        self._num_slots = _num_slots
        self._slot_class = slot_class
        self._slot_size = ctypes.sizeof(slot_class)
        
        #Allocate aligned buffer
        raw_bytes = _num_slots * self._slot_size + 63
        self._raw_buffer = ctypes.create_string_buffer(raw_bytes)
        raw_addr = ctypes.addressof(self._raw_buffer)
        self._slots_base = (raw_addr + 63) & ~63
        
        SlotArray = slot_class * _num_slots
        self.slots = SlotArray.from_address(self._slots_base)
        self._state = LocalQueueState(_num_slots)
    
    def batch_copy_from(self, src_slots, src_head: int, src_mask: int, count: int) -> int:
        '''
        Copy batch from shared queue slots to local queue.
        Single memmove for contiguous region, two for wrap-around.
        
        Args:
            src_slots: Source slot array (from SharedTaskQueue)
            src_head: Starting index in source
            src_mask: Mask for circular buffer (num_slots - 1)
            count: Number of slots to copy
            
        Returns:
            Number of slots actually copied
        '''
        state = self._state
        available = self._num_slots - state.curr_size
        
        if available < count:
            count = available
        
        if count <= 0:
            return 0
        
        slot_size = self._slot_size
        src_num_slots = src_mask + 1
        
        #Check if source wraps around
        if src_head + count <= src_num_slots:
            #No wrap: single memmove
            ctypes.memmove(
                ctypes.byref(self.slots[state.tail]),
                ctypes.byref(src_slots[src_head]),
                count * slot_size
            )
        else:
            #Wrap-around: two memmoves
            first_part = src_num_slots - src_head
            second_part = count - first_part
            
            #Copy first part (src_head to end of src buffer)
            ctypes.memmove(
                ctypes.byref(self.slots[state.tail]),
                ctypes.byref(src_slots[src_head]),
                first_part * slot_size
            )
            
            #Copy second part (start of src buffer)
            ctypes.memmove(
                ctypes.byref(self.slots[state.tail + first_part]),
                ctypes.byref(src_slots[0]),
                second_part * slot_size
            )
        
        state.tail = (state.tail + count) & state.mask
        state.curr_size += count
        return count

    def enqueue(self, slot) -> bool:
        state = self._state
        
        if state.curr_size >= state._num_slots:
            return False
        
        tail = state.tail
        ctypes.memmove(
            ctypes.byref(self.slots[tail]),
            ctypes.byref(slot),
            self._slot_size
        )
        state.tail = (tail + 1) & state.mask
        state.curr_size += 1
        return True

    def dequeue(self):
        state = self._state
        
        if state.curr_size == 0:
            return None
        
        head = state.head
        slot = self.slots[head]
        state.head = (head + 1) & state.mask
        state.curr_size -= 1
        return slot
    
    def get(self, index: int):
        '''Get slot at index (relative to head).'''
        state = self._state
        if index < 0 or index >= state.curr_size:
            return None
        actual_idx = (state.head + index) & state.mask
        return self.slots[actual_idx]
    
    def clear(self):
        '''Reset queue to empty state.'''
        state = self._state
        state.head = 0
        state.tail = 0
        state.curr_size = 0

    def is_empty(self) -> bool:
        return self._state.curr_size == 0

    def is_full(self) -> bool:
        return self._state.curr_size >= self._state._num_slots

    def count(self) -> int:
        return self._state.curr_size