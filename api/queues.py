
import ctypes
import time
from typing import Type

from .config import SlotConfig, SyncGroup
from .errors import ErrorCode, Component, format_error


class SharedQueueState(ctypes.Structure):
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
        ("debug_counter", ctypes.c_int64),
        ("_padding", ctypes.c_uint8 * 4),
    ]

SHARED_QUEUE_STATE_SIZE = ctypes.sizeof(SharedQueueState)

class LocalQueueState:
    __slots__ = ("head", "tail", "_num_slots", "mask", "curr_size")

    def __init__(self, _num_slots: int):
        self.head = 0
        self.tail = 0
        self._num_slots = _num_slots
        self.mask = _num_slots - 1
        self.curr_size = 0


class SharedTaskQueue:
    """Ring-buffer shared queue backed by shared memory. """
    DEFAULT_NUM_SLOTS = 4096

    def __init__(self,
                 queue_id: int,
                 num_slots: int,
                 slot_cfg: SlotConfig,
                 sync: SyncGroup,
                 queue_name: str = "shared_queue"):

        self.queue_id = queue_id
        self.queue_name = queue_name

        # Full slot config
        self._slot_cfg = slot_cfg
        self._num_slots = num_slots
        self._slot_size = slot_cfg.size

        #Cache  for hot-path
        self.lock = sync.lock
        self.batch_commit_lock = sync.batch_lock
        self._log_queue = sync.log_queue

        #Allocate slot buffer in shared memory
        from multiprocessing.shared_memory import SharedMemory
        slot_buffer_size = num_slots * self._slot_size
        self._shm_slots = SharedMemory(create=True, size=slot_buffer_size)

        SlotArray = slot_cfg.make_array(num_slots)
        self.slots = SlotArray.from_buffer(self._shm_slots.buf)

        self._shm_state = SharedMemory(create=True, size=SHARED_QUEUE_STATE_SIZE)
        self._state = SharedQueueState.from_buffer(self._shm_state.buf)
        self._init_state(num_slots)
        self.shm_slots_name = self._shm_slots.name
        self.shm_state_name = self._shm_state.name

    def _init_state(self, num_slots: int):
        """Zero-init all state fields."""
        s = self._state
        s._num_slots = num_slots
        s.mask = num_slots - 1
        s.head = 0
        s.tail = 0
        s.logical_occupancy = 0
        s.active_batches = 0
        s.batch_accumulation_counter = 0
        s.committed_accumulation = 0
        s.is_committed = 0
        s.num_batch_participants = 0
        s.active_worker_count = 0
        s.available_consumer_ids = 0
        s.debug_counter = 0

    @property
    def slot_cfg(self) -> SlotConfig:
        return self._slot_cfg

    @property
    def slot_class(self) -> Type[ctypes.Structure]:
        return self._slot_cfg.cls

    @property
    def slot_size(self) -> int:
        return self._slot_size
    
####### THE BELOW IS THE TESTED ENQUEUING METHOD DO NOT MODIFY    
    def _try_enqueue(self, slot) -> bool:
        state = self._state

        if state.is_committed:
            with self.batch_commit_lock:
                state.logical_occupancy -= state.committed_accumulation
                state.committed_accumulation = 0
                state.is_committed = 0

        if state.logical_occupancy >= self._num_slots:
            return False

        tail = state.tail
        ctypes.memmove(ctypes.byref(self.slots[tail]), ctypes.byref(slot), self._slot_size)
        state.tail = (tail + 1) & state.mask
        state.logical_occupancy += 1
        return True

    def enqueue(self, slot) -> bool:
        success = self._try_enqueue(slot)
        if not success and self._log_queue:
            err = format_error(ErrorCode.E001_QUEUE_FULL, Component.SHARED_QUEUE,
                               f"Queue {self.queue_id} full")
            try:
                self._log_queue.put((-1, err))
            except:
                pass
        return success
##### THE ABOVE IS THE TESTED ENQUEUING METHOD DO NOT MODIFY

### BACK PRESSURE
    def enqueue_blocking(self, slot, max_wait: float = 10.0) -> bool:
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
            err = format_error(ErrorCode.E001_QUEUE_FULL, Component.SHARED_QUEUE,
                               f"Queue {self.queue_id} timeout")
            try:
                self._log_queue.put((-1, err))
            except:
                pass
        return False

    def get_actual_occupancy(self) -> int:
        state = self._state
        return (state.tail - state.head) & state.mask

    def get_active_worker_count(self) -> int:
        return self._state.active_worker_count

    def is_empty(self) -> bool:
        return self._state.logical_occupancy == 0

    def get_debug_counter(self) -> int:
        return self._state.debug_counter

    def set_debug_counter(self, value: int):
        self._state.debug_counter = value

    def reset_debug_counter(self):
        self._state.debug_counter = 0

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
        if consumer_id < 0 or consumer_id >= 64:
            return
        state = self._state
        with self.lock:
            state.available_consumer_ids |= (1 << consumer_id)
            if state.active_worker_count > 0:
                state.active_worker_count -= 1

    ##CLEANUP FOR SHARED QUEUE
    def cleanup(self):
        del self.slots
        del self._state

        for shm in [self._shm_slots, self._shm_state]:
            try:
                shm.close()
            except Exception:
                pass
            try:
                shm.unlink()
            except Exception:
                pass


# ============================================================
# LOCAL TASK QUEUE
# ============================================================
class LocalTaskQueue:
    DEFAULT_NUM_SLOTS = 96

    def __init__(self, num_slots: int = None, slot_cfg: SlotConfig = None):
        num_slots = num_slots or self.DEFAULT_NUM_SLOTS

        self._num_slots = num_slots
        self._slot_class = slot_cfg.cls
        self._slot_size = slot_cfg.size

        raw_bytes = num_slots * self._slot_size + 63
        self._raw_buffer = ctypes.create_string_buffer(raw_bytes)
        raw_addr = ctypes.addressof(self._raw_buffer)
        self._slots_base = (raw_addr + 63) & ~63

        SlotArray = self._slot_class * num_slots
        self.slots = SlotArray.from_address(self._slots_base)
        self._state = LocalQueueState(num_slots)

    def batch_copy_from(self, src_slots, src_head: int, src_mask: int, count: int) -> int:
        state = self._state
        available = self._num_slots - state.curr_size

        if available < count:
            count = available
        if count <= 0:
            return 0

        slot_size = self._slot_size
        src_num_slots = src_mask + 1

        if src_head + count <= src_num_slots:
            ctypes.memmove(ctypes.byref(self.slots[state.tail]),
                           ctypes.byref(src_slots[src_head]),
                           count * slot_size)
        else:
            first_part = src_num_slots - src_head
            second_part = count - first_part
            ctypes.memmove(ctypes.byref(self.slots[state.tail]),
                           ctypes.byref(src_slots[src_head]),
                           first_part * slot_size)
            ctypes.memmove(ctypes.byref(self.slots[state.tail + first_part]),
                           ctypes.byref(src_slots[0]),
                           second_part * slot_size)

        state.tail = (state.tail + count) & state.mask
        state.curr_size += count
        return count

    def enqueue(self, slot) -> bool:
        state = self._state
        if state.curr_size >= state._num_slots:
            return False
        tail = state.tail
        ctypes.memmove(ctypes.byref(self.slots[tail]), ctypes.byref(slot), self._slot_size)
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
        state = self._state
        if index < 0 or index >= state.curr_size:
            return None
        return self.slots[(state.head + index) & state.mask]

    def clear(self):
        state = self._state
        state.head = 0
        state.tail = 0
        state.curr_size = 0

    #HELPERS
    def is_empty(self) -> bool:
        return self._state.curr_size == 0
    def is_full(self) -> bool:
        return self._state.curr_size >= self._state._num_slots
    def count(self) -> int:
        return self._state.curr_size