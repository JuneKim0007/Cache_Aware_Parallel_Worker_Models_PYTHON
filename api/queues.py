# ============================================================
# API/QUEUES.PY — Shared and local task queues
# ============================================================
# SharedMemory lifecycle (supervisor-owned):
#   release_views() → del ctypes arrays
#   close()         → close shm handles
#   unlink()        → remove shm segments from OS
# ============================================================

import ctypes
import time
from multiprocessing import Lock, Queue
from multiprocessing.shared_memory import SharedMemory

from .slot import TaskSlot
from .errors import ErrorCode, Component, format_error


# ============================================================
# SHARED QUEUE STATE
# ============================================================
class SharedQueueState(ctypes.Structure):
    _fields_ = [
        ("head",                       ctypes.c_uint32),
        ("tail",                       ctypes.c_uint32),
        ("_num_slots",                 ctypes.c_uint32),
        ("mask",                       ctypes.c_uint32),
        ("logical_occupancy",          ctypes.c_uint32),
        ("active_batches",             ctypes.c_uint64),
        ("batch_accumulation_counter", ctypes.c_uint32),
        ("committed_accumulation",     ctypes.c_uint32),
        ("is_committed",               ctypes.c_uint8),
        ("num_batch_participants",     ctypes.c_uint8),
        ("active_worker_count",        ctypes.c_uint8),
        ("_pad1",                      ctypes.c_uint8),
        ("available_consumer_ids",     ctypes.c_uint64),
        ("debug_counter",              ctypes.c_int64),
        ("_padding",                   ctypes.c_uint8 * 4),
    ]

SHARED_QUEUE_STATE_SIZE = ctypes.sizeof(SharedQueueState)


# ============================================================
# LOCAL QUEUE STATE
# ============================================================
class LocalQueueState:
    __slots__ = ("head", "tail", "_num_slots", "mask", "curr_size")

    def __init__(self, _num_slots):
        self.head = 0
        self.tail = 0
        self._num_slots = _num_slots
        self.mask = _num_slots - 1
        self.curr_size = 0


# ============================================================
# SHARED TASK QUEUE
# ============================================================
class SharedTaskQueue:
    DEFAULT_NUM_SLOTS = 4096

    def __init__(self, queue_id, _num_slots=256, num_workers=1,
                 queue_name="shared_queue", lock=None, batch_lock=None,
                 log_queue=None):

        self.queue_id = queue_id
        self.queue_name = queue_name
        self._num_slots = _num_slots
        self._slot_size = ctypes.sizeof(TaskSlot)
        self._log_queue = log_queue

        slot_buffer_size = _num_slots * self._slot_size
        self._shm_slots = SharedMemory(create=True, size=slot_buffer_size)

        SlotArray = TaskSlot * _num_slots
        self.slots = SlotArray.from_buffer(self._shm_slots.buf)

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
        self._state.debug_counter = 0

        self.lock = lock or Lock()
        self.batch_commit_lock = batch_lock or Lock()

        self.shm_slots_name = self._shm_slots.name
        self.shm_state_name = self._shm_state.name

    @property
    def slot_size(self):
        return self._slot_size

    def _try_enqueue(self, slot):
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

    def enqueue(self, slot):
        success = self._try_enqueue(slot)
        if not success and self._log_queue:
            err = format_error(ErrorCode.E001_QUEUE_FULL, Component.SHARED_QUEUE,
                               f"Queue {self.queue_id} full")
            try:
                self._log_queue.put((-1, err))
            except:
                pass
        return success

    def enqueue_blocking(self, slot, max_wait=10.0):
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

    def get_actual_occupancy(self):
        return (self._state.tail - self._state.head) & self._state.mask

    def get_active_worker_count(self):
        return self._state.active_worker_count

    def is_empty(self):
        return self._state.logical_occupancy == 0

    def get_debug_counter(self):
        return self._state.debug_counter

    def set_debug_counter(self, value):
        self._state.debug_counter = value

    def reset_debug_counter(self):
        self._state.debug_counter = 0

    def allocate_consumer_id(self):
        state = self._state
        with self.lock:
            if state.available_consumer_ids == 0:
                return -1
            avail = state.available_consumer_ids
            consumer_id = (avail & -avail).bit_length() - 1
            state.available_consumer_ids &= ~(1 << consumer_id)
            state.active_worker_count += 1
            return consumer_id

    def release_consumer_id(self, consumer_id):
        if consumer_id < 0 or consumer_id >= 64:
            return
        state = self._state
        with self.lock:
            state.available_consumer_ids |= (1 << consumer_id)
            if state.active_worker_count > 0:
                state.active_worker_count -= 1

    # ==========================================================
    # THREE-PHASE CLEANUP (call in order, supervisor only)
    # ==========================================================
    def release_views(self):
        """Phase 1: Delete ctypes views that hold buffer references."""
        try:
            del self.slots
        except AttributeError:
            pass
        try:
            del self._state
        except AttributeError:
            pass

    def close(self):
        """Phase 2: Close SharedMemory file descriptors."""
        for shm in (self._shm_slots, self._shm_state):
            try:
                shm.close()
            except Exception:
                pass

    def unlink(self):
        """Phase 3: Remove SharedMemory segments from OS. Supervisor only."""
        for shm in (self._shm_slots, self._shm_state):
            try:
                shm.unlink()
            except Exception:
                pass

    def cleanup(self):
        """Convenience: all three phases. Supervisor only, after workers joined."""
        self.release_views()
        self.close()
        self.unlink()


# ============================================================
# LOCAL TASK QUEUE
# ============================================================
class LocalTaskQueue:
    DEFAULT_NUM_SLOTS = 96

    def __init__(self, _num_slots=None):
        _num_slots = _num_slots or self.DEFAULT_NUM_SLOTS

        self._num_slots = _num_slots
        self._slot_size = ctypes.sizeof(TaskSlot)

        raw_bytes = _num_slots * self._slot_size + 63
        self._raw_buffer = ctypes.create_string_buffer(raw_bytes)
        raw_addr = ctypes.addressof(self._raw_buffer)
        self._slots_base = (raw_addr + 63) & ~63

        SlotArray = TaskSlot * _num_slots
        self.slots = SlotArray.from_address(self._slots_base)
        self._state = LocalQueueState(_num_slots)

    def batch_copy_from(self, src_slots, src_head, src_mask, count):
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

    def get(self, index):
        state = self._state
        if index < 0 or index >= state.curr_size:
            return None
        return self.slots[(state.head + index) & state.mask]

    def clear(self):
        state = self._state
        state.head = 0
        state.tail = 0
        state.curr_size = 0

    def is_empty(self):
        return self._state.curr_size == 0

    def count(self):
        return self._state.curr_size