#!/usr/bin/env python3
# ============================================================
# MAIN.PY - Usage Example
# ============================================================
# Example usage of the multiprocessing task queue system.
# ============================================================

import warnings
warnings.filterwarnings("ignore", message=".*resource_tracker.*")

from slot import TaskSlot128, ProcTaskFnID
from allocation import allocate
from supervisor import SupervisorController


def main():
    '''
    Example: Full task processing with terminal display.
    '''
    #========================================================
    # CONFIGURATION
    #========================================================
    NUM_WORKERS = 4
    QUEUE_SLOTS = 256*4
    TASK_COUNT = 600
    DEBUG_DELAY = 0.1
    ADMIN_FREQUENCY = 5
    
    #========================================================
    # ALLOCATE
    #========================================================
    print("=" * 50)
    print("  MULTIPROCESSING TASK QUEUE")
    print("=" * 50)
    print()
    print(f"Allocating: {NUM_WORKERS} workers, {QUEUE_SLOTS} slots")
    
    result = allocate(
        num_workers=NUM_WORKERS,
        queue_slots=QUEUE_SLOTS,
        slot_class=TaskSlot128,
        queue_name="main_queue",
        debug_task_delay=DEBUG_DELAY,
        admin_frequency=ADMIN_FREQUENCY
    )
    
    #========================================================
    # ENQUEUE TASKS
    #========================================================
    print(f"Enqueueing {TASK_COUNT} tasks...")
    
    for i in range(TASK_COUNT):
        task = TaskSlot128()
        task.tsk_id = i + 1
        task.fn_id = ProcTaskFnID.INCREMENT
        task.args[0] = i * 10
        task.args[1] = 1
        result.queue.enqueue(task)
    
    print(f"Queue ready. Starting supervisor...")
    print()
    
    #========================================================
    # RUN SUPERVISOR
    #========================================================
    supervisor = SupervisorController(
        shared_queue=result.queue,
        status_shm=result.status_shm,
        processes=result.processes,
        log_queue=result.log_queue,
        num_workers=result.num_workers,
        use_display=True
    )
    
    return supervisor.run()


def simple_example():
    '''
    Minimal example without display.
    '''
    print("Simple example (no display)")
    print()
    
    result = allocate(num_workers=2, queue_slots=64)
    
    #Enqueue tasks
    for i in range(63):
        t = TaskSlot128()
        t.tsk_id = i + 1
        t.fn_id = ProcTaskFnID.INCREMENT
        result.queue.enqueue(t)
    
    #Run supervisor
    supervisor = SupervisorController(
        shared_queue=result.queue,
        status_shm=result.status_shm,
        processes=result.processes,
        log_queue=result.log_queue,
        num_workers=result.num_workers,
        use_display=False
    )
    
    return supervisor.run()


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "--simple":
        exit(simple_example())
    else:
        exit(main())