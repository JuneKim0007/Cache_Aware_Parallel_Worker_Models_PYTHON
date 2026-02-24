# ============================================================
# API/WORKER.PY — Worker process entry
# ============================================================
# SharedMemory ownership model:
#   SUPERVISOR creates segments  → owns → close + unlink
#   WORKERS    attach segments   → borrow → untrack + close only
#
# Workers call _untrack_shm() immediately after attaching.
# This prevents:
#   1. resource_tracker "leaked shared_memory" warnings
#   2. __del__ trying to close buffers with live ctypes views
#   3. Worker accidentally unlinking segments it doesn't own
# ============================================================

import os
import sys
import gc
import ctypes
import time
import inspect
import warnings
from multiprocessing import Lock, Queue
from multiprocessing.shared_memory import SharedMemory

from .slot import TaskSlot, ProcTaskFnID, SLOT_SIZE
from .queues import SharedQueueState, LocalTaskQueue
from .tasks import TaskDispatcher, TaskResult
from .types import resolve_handler, HandlerMeta


# ============================================================
# SHARED MEMORY ATTACH (untracked)
# ============================================================
def _attach_shm_untracked(name):
    """Attach to existing SharedMemory WITHOUT registering with resource_tracker.
    
    Workers don't own segments — supervisor handles unlink.
    By preventing registration, we avoid:
      - resource_tracker "leaked" warnings at worker exit
      - Double-unregister KeyError (worker untrack + supervisor unlink)
      - __del__ calling close() on tracked segments during GC
      
    Works on both fork (shared tracker) and spawn (separate tracker).
    """
    import multiprocessing.resource_tracker as _rt
    _orig_register = _rt.register
    _rt.register = lambda *a, **kw: None  # no-op during attach
    try:
        shm = SharedMemory(name=name, create=False)
    finally:
        _rt.register = _orig_register  # restore immediately
    return shm


# ============================================================
# WORKER STATUS
# ============================================================
class WorkerStatusStruct(ctypes.Structure):
    _pack_ = 1
    _fields_ = [
        ("state",             ctypes.c_uint8),
        ("_pad0",             ctypes.c_uint8 * 3),
        ("local_queue_count", ctypes.c_uint32),
        ("completed_tasks",   ctypes.c_uint32),
        ("_padding",          ctypes.c_uint8 * 52),
    ]

WORKER_STATUS_SIZE = ctypes.sizeof(WorkerStatusStruct)

STATE_INIT       = 0
STATE_RUNNING    = 1
STATE_IDLE       = 2
STATE_TERMINATED = 3

STATE_NAMES = {
    STATE_INIT:       "INIT",
    STATE_RUNNING:    "RUNNING",
    STATE_IDLE:       "IDLE",
    STATE_TERMINATED: "TERMINATED",
}


# ============================================================
# WORKER CONTEXT
# ============================================================
class WorkerContext:
    __slots__ = (
        "worker_id", "consumer_id",
        "shm_slots_name", "shm_state_name", "shm_status_name",
        "_num_slots", "lock", "batch_lock", "log_queue",
        "supervisor_pid", "debug_task_delay",
        "admin_frequency", "handler_module",
        "worker_batch_size", "pool_proxy",
        "handlers_map",
    )

    def __init__(self, worker_id, consumer_id,
                 shm_slots_name, shm_state_name, shm_status_name,
                 _num_slots, lock, batch_lock, log_queue,
                 supervisor_pid=0, debug_task_delay=0.0,
                 admin_frequency=5, handler_module=None,
                 worker_batch_size=256, pool_proxy=None,
                 handlers_map=None):
        self.worker_id = worker_id
        self.consumer_id = consumer_id
        self.shm_slots_name = shm_slots_name
        self.shm_state_name = shm_state_name
        self.shm_status_name = shm_status_name
        self._num_slots = _num_slots
        self.lock = lock
        self.batch_lock = batch_lock
        self.log_queue = log_queue
        self.supervisor_pid = supervisor_pid
        self.debug_task_delay = debug_task_delay
        self.admin_frequency = admin_frequency
        self.handler_module = handler_module
        self.worker_batch_size = worker_batch_size
        self.pool_proxy = pool_proxy
        self.handlers_map = handlers_map


# ============================================================
# WORKER-LOCAL POOL WRAPPER
# ============================================================
class _WorkerPoolProxy:
    """Lightweight pool wrapper for worker processes.
    store() raises — workers only retrieve and remove."""
    __slots__ = ("_proxy",)

    def __init__(self, proxy):
        self._proxy = proxy

    def store(self, data):
        raise RuntimeError("Workers should not store to ArgsPool")

    def retrieve(self, pool_id):
        return self._proxy.get(pool_id)

    def remove(self, pool_id):
        try:
            del self._proxy[pool_id]
        except KeyError:
            pass


# ============================================================
# HANDLER LOADING — typed only
# ============================================================
def _load_from_map(handlers_map, dispatcher, pool_wrapper, log_queue, worker_id):
    """Load handlers from directly-passed map (from @app.task registration)."""
    loaded = 0
    for fn_id, handler in handlers_map.items():
        try:
            specs = resolve_handler(handler)
            meta = HandlerMeta(
                fn_id=fn_id, name=handler.__name__,
                handler=handler, specs=specs, pool=pool_wrapper,
            )
            dispatcher.register(meta)
            loaded += 1
        except Exception as e:
            log_queue.put((worker_id,
                f"[Error] Cannot resolve {handler.__name__} "
                f"({fn_id:#06x}): {e}"))
    return loaded


def _load_from_module(mod, dispatcher, pool_wrapper, log_queue, worker_id):
    """Load handlers from module's HANDLERS dict."""
    if not hasattr(mod, 'HANDLERS'):
        return 0

    loaded = 0
    for fn_id, handler in mod.HANDLERS.items():
        if dispatcher._handlers.get(fn_id) is not None:
            continue
        try:
            specs = resolve_handler(handler)
            meta = HandlerMeta(
                fn_id=fn_id, name=handler.__name__,
                handler=handler, specs=specs, pool=pool_wrapper,
            )
            dispatcher.register(meta)
            loaded += 1
        except Exception as e:
            log_queue.put((worker_id,
                f"[Error] Cannot resolve {handler.__name__} "
                f"({fn_id:#06x}): {e}. SKIPPED."))
    return loaded


# ============================================================
# SUPERVISOR CHECK
# ============================================================
def is_supervisor_alive(pid):
    if pid <= 0:
        return True
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


# ============================================================
# WORKER PROCESS ENTRY
# ============================================================
def worker_process_entry(ctx: WorkerContext):
    warnings.filterwarnings("ignore", category=UserWarning)

    supervisor_pid = ctx.supervisor_pid

    # ---- ATTACH SHARED MEMORY (untracked — supervisor owns these) ----
    shm_slots = shm_state = shm_status = None
    try:
        shm_slots  = _attach_shm_untracked(ctx.shm_slots_name)
        shm_state  = _attach_shm_untracked(ctx.shm_state_name)
        shm_status = _attach_shm_untracked(ctx.shm_status_name)
    except Exception as e:
        ctx.log_queue.put((-1, f"[Error][Worker] Worker {ctx.worker_id} attach failed: {e}"))
        return

    # ---- CREATE CTYPES VIEWS ----
    SlotArray = TaskSlot * ctx._num_slots
    slots = SlotArray.from_buffer(shm_slots.buf)
    state = SharedQueueState.from_buffer(shm_state.buf)

    StatusArray = WorkerStatusStruct * 64
    status_array = StatusArray.from_buffer(shm_status.buf)
    my_status = status_array[ctx.consumer_id]

    my_status.state = STATE_RUNNING
    my_status.local_queue_count = 0
    my_status.completed_tasks = 0

    max_batch = ctx.worker_batch_size
    local_queue = LocalTaskQueue(_num_slots=max_batch)

    dispatcher = TaskDispatcher()

    # ---- LOAD HANDLERS ----
    pool_wrapper = _WorkerPoolProxy(ctx.pool_proxy) if ctx.pool_proxy else None
    handler_count = 0

    if ctx.handlers_map:
        handler_count += _load_from_map(
            ctx.handlers_map, dispatcher, pool_wrapper,
            ctx.log_queue, ctx.worker_id)

    if ctx.handler_module:
        try:
            import importlib
            mod = importlib.import_module(ctx.handler_module)
            handler_count += _load_from_module(
                mod, dispatcher, pool_wrapper,
                ctx.log_queue, ctx.worker_id)
        except Exception as e:
            ctx.log_queue.put((-1, f"[Error] Failed to load {ctx.handler_module}: {e}"))

    ctx.log_queue.put((ctx.worker_id, f"Loaded {handler_count} handlers"))

    # ---- ADMIN/LOG HELPERS ----
    tasks_since_admin = 0
    admin_freq = ctx.admin_frequency

    def do_admin_check():
        nonlocal tasks_since_admin
        tasks_since_admin = 0
        if not is_supervisor_alive(supervisor_pid):
            ctx.log_queue.put((ctx.worker_id, "Supervisor dead"))
            return False
        return True

    def throttled_log(msg):
        nonlocal tasks_since_admin
        tasks_since_admin += 1
        if tasks_since_admin >= admin_freq:
            ctx.log_queue.put((ctx.worker_id, msg))
            do_admin_check()

    task_delay = ctx.debug_task_delay
    consumer_bit = 1 << ctx.consumer_id
    running = True
    idle_count = 0

    # ============================================================
    # MAIN LOOP
    # ============================================================
    while running:
        batch_size = 0
        batch_head = 0

        #### BATCH LOGIC — INTERNAL, TESTED — DO NOT CHANGE ####
        with ctx.lock:
            available = (state.tail - state.head) & state.mask
            if available > 0:
                batch_size = min(available, max_batch)
                batch_head = state.head
                state.head = (state.head + batch_size) & state.mask
                state.batch_accumulation_counter += batch_size
                state.num_batch_participants += 1
                state.active_batches |= consumer_bit

        if batch_size == 0:
            my_status.state = STATE_IDLE
            idle_count += 1

            if idle_count % 100 == 0:
                if not is_supervisor_alive(supervisor_pid):
                    ctx.log_queue.put((ctx.worker_id, "Supervisor dead"))
                    running = False
                    break

            if idle_count > 1000:
                if state.tail == state.head and state.logical_occupancy == 0:
                    break

            time.sleep(0.001)
            continue

        idle_count = 0
        my_status.state = STATE_RUNNING

        local_queue.clear()
        local_queue.batch_copy_from(slots, batch_head, state.mask, batch_size)
        my_status.local_queue_count = batch_size

        for i in range(batch_size):
            slot = local_queue.get(i)
            my_status.local_queue_count = batch_size - i

            if slot.fn_id == ProcTaskFnID.TERMINATE:
                running = False
                my_status.completed_tasks += 1
                ctx.log_queue.put((ctx.worker_id, f"TERMINATE tsk_id={slot.tsk_id}"))
                continue

            if task_delay > 0:
                time.sleep(task_delay)

            result = dispatcher.dispatch(slot)
            my_status.completed_tasks += 1

            if slot.fn_id == ProcTaskFnID.INCREMENT:
                with ctx.lock:
                    state.debug_counter += 1

            if not result.success:
                ctx.log_queue.put((ctx.worker_id, f"[Error] tsk_id={slot.tsk_id}: {result.error}"))

        my_status.local_queue_count = 0

        bitmap_zero = False
        with ctx.lock:
            state.active_batches &= ~consumer_bit
            bitmap_zero = (state.active_batches == 0)

        if bitmap_zero:
            with ctx.batch_lock:
                state.committed_accumulation += state.batch_accumulation_counter
                state.batch_accumulation_counter = 0
                state.num_batch_participants = 0
                state.is_committed = 1
        #### END BATCH LOGIC ####

    # ============================================================
    # CLEANUP
    # ============================================================
    # Mark terminated (still writing to shm — views alive)
    my_status.state = STATE_TERMINATED

    with ctx.lock:
        state.available_consumer_ids |= consumer_bit
        if state.active_worker_count > 0:
            state.active_worker_count -= 1

    # Release all ctypes views BEFORE closing shm
    del slots
    del state
    del status_array
    del my_status
    del local_queue

    # Force GC to drop internal buffer references from ctypes
    gc.collect()

    # Close shm handles. Since we untracked, __del__ is a no-op.
    # Supervisor will unlink after all workers have joined.
    for shm in (shm_slots, shm_state, shm_status):
        if shm is not None:
            try:
                shm.close()
            except BufferError:
                pass  # rare: ctypes ref survived GC — harmless, supervisor will unlink
            except Exception:
                pass