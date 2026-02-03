# ============================================================
# WORKER.PY
# ============================================================

#===
# the total task completed will be always higher than the task enqueued and this is because...
# termination is also a signal.
# supervisor touches local_queue enqueuing directly

import os
import sys
import ctypes
import time
import warnings
from multiprocessing import Lock, Queue
from multiprocessing.shared_memory import SharedMemory
from typing import Optional

from slot import (TaskSlot128, TaskSlot128_cargs, TaskSlot196, TaskSlot196_cargs,
                  ProcTaskFnID)
from queues import SharedQueueState


#============================================================
# WORKER STATUS STRUCTURE
#============================================================
class WorkerStatusStruct(ctypes.Structure):
    '''
    Per-worker status in shared memory.
    Each worker writes to its slot; supervisor reads.
    64 bytes aligned for cache efficiency.
    '''
    _pack_ = 1
    _fields_ = [
        ("state", ctypes.c_uint8),
        ("_pad0", ctypes.c_uint8 * 3),
        ("local_queue_count", ctypes.c_uint32),
        ("completed_tasks", ctypes.c_uint32),
        ("_padding", ctypes.c_uint8 * 52),
    ]

WORKER_STATUS_SIZE = ctypes.sizeof(WorkerStatusStruct)
assert WORKER_STATUS_SIZE == 64, f"WorkerStatusStruct must be 64 bytes, got {WORKER_STATUS_SIZE}"

#Worker states
STATE_INIT = 0
STATE_RUNNING = 1
STATE_IDLE = 2
STATE_TERMINATED = 3

STATE_NAMES = {
    STATE_INIT: "INIT",
    STATE_RUNNING: "RUNNING",
    STATE_IDLE: "IDLE",
    STATE_TERMINATED: "TERMINATED"
}


#============================================================
# SLOT CLASS LOOKUP
#============================================================
SLOT_CLASS_MAP = {
    "TaskSlot128": TaskSlot128,
    "TaskSlot196": TaskSlot196,
    "TaskSlot128_cargs": TaskSlot128_cargs,
    "TaskSlot196_cargs": TaskSlot196_cargs,
}


#============================================================
# WORKER CONTEXT
#============================================================
class WorkerContext:
    '''
    Context passed to worker process containing shared memory references.
    Must be picklable for multiprocessing.
    
    supervisor_pid: PID of supervisor process (for orphan detection)
    admin_frequency: Admin check frequency (log + supervisor alive check)
    '''
    def __init__(self,
                 worker_id: int,
                 consumer_id: int,
                 shm_slots_name: str,
                 shm_state_name: str,
                 shm_status_name: str,
                 slot_class_name: str,
                 _num_slots: int,
                 slot_size: int,
                 lock: Lock,
                 batch_lock: Lock,
                 log_queue: Queue,
                 supervisor_pid: int = 0,
                 debug_task_delay: float = 0.0,
                 admin_frequency: int = 5,
                 arg_terminator: bytes = b'\x00',
                 arg_delimiter: bytes = b' '):
        self.worker_id = worker_id
        self.consumer_id = consumer_id
        self.shm_slots_name = shm_slots_name
        self.shm_state_name = shm_state_name
        self.shm_status_name = shm_status_name
        self.slot_class_name = slot_class_name
        self._num_slots = _num_slots
        self.slot_size = slot_size
        self.lock = lock
        self.batch_lock = batch_lock
        self.log_queue = log_queue
        self.supervisor_pid = supervisor_pid
        self.debug_task_delay = debug_task_delay
        self.admin_frequency = admin_frequency
        self.arg_terminator = arg_terminator
        self.arg_delimiter = arg_delimiter


#============================================================
# SUPERVISOR CHECK
#============================================================
def is_supervisor_alive(pid: int) -> bool:
    '''Check if supervisor process is still alive.'''
    if pid <= 0:
        return True  #No supervisor PID provided, assume alive
    try:
        os.kill(pid, 0)  #Signal 0 just checks existence
        return True
    except OSError:
        return False


#============================================================
# WORKER PROCESS ENTRY
#============================================================
def worker_process_entry(ctx: WorkerContext):
    '''
    Entry point for worker process.
    
    Workflow:
    1. Claim batch (bitmap flip + head advance) under lock
    2. Copy batch to local queue (single memmove, no lock)
    3. Process tasks from local queue
    4. Finish batch (bitmap clear, commit if last)
    '''
    from tasks import TaskDispatcher, TaskContext as TCtx
    from args import ArgParser
    from queues import LocalTaskQueue
    
    #Suppress warnings
    warnings.filterwarnings("ignore", category=UserWarning)
    
    supervisor_pid = ctx.supervisor_pid
    
    #Attach to shared memories
    try:
        shm_slots = SharedMemory(name=ctx.shm_slots_name)
        shm_state = SharedMemory(name=ctx.shm_state_name)
        shm_status = SharedMemory(name=ctx.shm_status_name)
    except Exception as e:
        ctx.log_queue.put((-1, f"[Error][Worker] Worker {ctx.worker_id} failed to attach: {e}"))
        return
    
    #Get slot class
    slot_class = SLOT_CLASS_MAP.get(ctx.slot_class_name)
    if slot_class is None:
        ctx.log_queue.put((-1, f"[Error][Worker] Unknown slot_class: {ctx.slot_class_name}"))
        return
    
    #Create views into shared memory
    SlotArray = slot_class * ctx._num_slots
    slots = SlotArray.from_buffer(shm_slots.buf)
    state = SharedQueueState.from_buffer(shm_state.buf)
    
    #Worker status
    StatusArray = WorkerStatusStruct * 64
    status_array = StatusArray.from_buffer(shm_status.buf)
    my_status = status_array[ctx.consumer_id]
    
    my_status.state = STATE_RUNNING
    my_status.local_queue_count = 0
    my_status.completed_tasks = 0
    
    #Local queue for batch processing
    max_batch = 16
    local_queue = LocalTaskQueue(_num_slots=max_batch, slot_class=slot_class)
    
    #Setup task dispatcher
    dispatcher = TaskDispatcher()
    arg_parser = ArgParser(terminator=ctx.arg_terminator, delimiter=ctx.arg_delimiter)
    
    #Admin frequency tracking
    tasks_since_admin = 0
    admin_freq = ctx.admin_frequency
    
    def do_admin_check() -> bool:
        '''Returns False if worker should terminate.'''
        nonlocal tasks_since_admin
        tasks_since_admin = 0
        if not is_supervisor_alive(supervisor_pid):
            ctx.log_queue.put((ctx.worker_id, "Supervisor dead, terminating"))
            return False
        return True
    
    def throttled_log(msg: str):
        '''Log with frequency throttling.'''
        nonlocal tasks_since_admin
        tasks_since_admin += 1
        if tasks_since_admin >= admin_freq:
            ctx.log_queue.put((ctx.worker_id, msg))
            do_admin_check()
    
    task_ctx = TCtx(
        worker_id=ctx.worker_id,
        arg_parser=arg_parser,
        log_func=throttled_log
    )
    
    task_delay = ctx.debug_task_delay
    consumer_bit = 1 << ctx.consumer_id
    running = True
    idle_count = 0
    
    #Main loop
    while running:
        #=== CLAIM BATCH (under lock) ===
        batch_size = 0
        batch_head = 0
        
        with ctx.lock:
            available = (state.tail - state.head) & state.mask
            if available > 0:
                batch_size = min(available, max_batch)
                batch_head = state.head
                
                #Advance head and set bitmap
                state.head = (state.head + batch_size) & state.mask
                state.batch_accumulation_counter += batch_size
                state.num_batch_participants += 1
                state.active_batches |= consumer_bit
        
        #=== IDLE CHECK ===
        if batch_size == 0:
            my_status.state = STATE_IDLE
            idle_count += 1
            
            if idle_count % 100 == 0:
                if not is_supervisor_alive(supervisor_pid):
                    ctx.log_queue.put((ctx.worker_id, "Supervisor dead, terminating"))
                    running = False
                    break
            
            if idle_count > 1000:
                if state.tail == state.head and state.logical_occupancy == 0:
                    break
            
            time.sleep(0.001)
            continue
        
        idle_count = 0
        my_status.state = STATE_RUNNING
        
        #=== COPY BATCH (no lock, single memmove) ===
        local_queue.clear()
        local_queue.batch_copy_from(slots, batch_head, state.mask, batch_size)
        my_status.local_queue_count = batch_size
        
        #=== PROCESS BATCH ===
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
            
            result = dispatcher.dispatch(slot, task_ctx)
            my_status.completed_tasks += 1
            
            if not result.success:
                ctx.log_queue.put((ctx.worker_id, f"[Error] tsk_id={slot.tsk_id}: {result.error}"))
        
        my_status.local_queue_count = 0
        
        #=== FINISH BATCH (bitmap clear) ===
        bitmap_zero = False
        with ctx.lock:
            state.active_batches &= ~consumer_bit
            bitmap_zero = (state.active_batches == 0)
        
        #If all batches done, commit to producer
        if bitmap_zero:
            with ctx.batch_lock:
                state.committed_accumulation += state.batch_accumulation_counter
                state.batch_accumulation_counter = 0
                state.num_batch_participants = 0
                state.is_committed = 1
    
    #=== CLEANUP ===
    my_status.state = STATE_TERMINATED
    
    with ctx.lock:
        state.available_consumer_ids |= consumer_bit
        if state.active_worker_count > 0:
            state.active_worker_count -= 1
    
    try:
        sys.stderr = open(os.devnull, 'w')
    except:
        pass