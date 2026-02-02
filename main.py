#!/usr/bin/env python3
# ============================================================
# MAIN.PY
# ============================================================

import os
import sys
import time
import ctypes
from multiprocessing.shared_memory import SharedMemory

from slot import TaskSlot128, ProcTaskFnID
from allocation import (
    allocate_system, start_workers, stop_workers, join_workers, cleanup_system,
    WorkerStatusStruct, STATE_NAMES, STATE_TERMINATED,
    WORKER_STATUS_SIZE
)


#============================================================
# TERMINAL CONTROL
#============================================================
def clear_screen():
    sys.stdout.write("\033[2J")    #Clear the screen
    sys.stdout.write("\033[H")
    sys.stdout.flush()

def move_cursor(row: int, col: int):
    sys.stdout.write(f"\033[{row};{col}H")
    sys.stdout.flush()

def clear_line():
    sys.stdout.write("\033[2K")
    sys.stdout.flush()

def hide_cursor():
    sys.stdout.write("\033[?25l")
    sys.stdout.flush()

def show_cursor():
    sys.stdout.write("\033[?25h")
    sys.stdout.flush()

def save_cursor():
    sys.stdout.write("\033[s")
    sys.stdout.flush()

def restore_cursor():
    sys.stdout.write("\033[u")
    sys.stdout.flush()


#============================================================
# SUPERVISOR DISPLAY
#============================================================
class SupervisorDisplay:
    '''
    Non-scrolling terminal display for supervisor.
    
    Layout:
    Row 1: Header
    Row 2: Separator
    Row 3: Shared Queue info
    Row 4: Separator
    Row 5+: Worker status (one per worker)
    Row N: Separator
    Row N+1: Footer with controls
    Row N+2+: Log area (scrolling allowed here)
    '''
    
    def __init__(self, num_workers: int, queue_id: int = 1):
        self.num_workers = num_workers
        self.queue_id = queue_id
        self.header_rows = 4
        self.worker_start_row = 5
        self.footer_row = self.worker_start_row + num_workers + 1
        self.log_start_row = self.footer_row + 2
        self.log_lines = []
        self.max_log_lines = 10
    
    def init_display(self):
        hide_cursor()
        clear_screen()
        
        #Header
        move_cursor(1, 1)
        sys.stdout.write("MultiProcessing")
        move_cursor(2, 1)
        sys.stdout.write("-" * 70)
        move_cursor(3, 1)
        sys.stdout.write(f"shared_queue {self.queue_id}")
        move_cursor(4, 1)
        sys.stdout.write("-" * 70)
        
        #Worker header
        move_cursor(self.worker_start_row, 1)
        sys.stdout.write(f"{'worker_id':<12}{'state':<15}{'local_queue':<15}{'completed':<15}")
        
        #Initialize worker lines
        for i in range(self.num_workers):
            row = self.worker_start_row + 1 + i
            move_cursor(row, 1)
            sys.stdout.write(f"{i:<12}{'INIT':<15}{0:<15}{0:<15}")
        
        #Footer
        move_cursor(self.footer_row, 1)
        sys.stdout.write("-" * 70)
        move_cursor(self.footer_row + 1, 1)
        sys.stdout.write("Queue: 0     | Total: 0      | Press Ctrl+C to stop")
        
        #Log header
        move_cursor(self.log_start_row, 1)
        sys.stdout.write("-" * 70)
        move_cursor(self.log_start_row + 1, 1)
        sys.stdout.write("Task Log:")
        
        sys.stdout.flush()
    
    def update_worker(self, worker_id: int, state: int, local_count: int, completed: int):
        row = self.worker_start_row + 1 + worker_id
        state_name = STATE_NAMES.get(state, "UNKNOWN")
        
        move_cursor(row, 1)
        clear_line()
        sys.stdout.write(f"{worker_id:<12}{state_name:<15}{local_count:<15}{completed:<15}")
        sys.stdout.flush()
    
    def update_footer(self, queue_occupancy: int, total_completed: int):
        move_cursor(self.footer_row + 1, 1)
        clear_line()
        sys.stdout.write(f"Queue: {queue_occupancy:<5} | Total: {total_completed:<6} | Press Ctrl+C to stop")
        sys.stdout.flush()
    
    def add_log(self, worker_id: int, message: str):
        self.log_lines.append(f"[W{worker_id}] {message}")
        if len(self.log_lines) > self.max_log_lines:
            self.log_lines.pop(0)
        
        #Redraw log area
        for i, line in enumerate(self.log_lines):
            row = self.log_start_row + 2 + i
            move_cursor(row, 1)
            clear_line()
            #Truncate long lines
            sys.stdout.write(line[:68])
        
        sys.stdout.flush()
    
    def finalize(self):
        '''Restore terminal state'''
        move_cursor(self.log_start_row + 2 + self.max_log_lines + 1, 1)
        show_cursor()


#============================================================
# MAIN
#============================================================
def main():
    clear_screen()
    print("=" * 50)
    print("  MULTIPROCESSING TASK SUPERVISOR")
    print("=" * 50)
    print()
    
    #Get number of workers
    while True:
        try:
            num_workers = int(input("Enter number of workers (1-64): "))
            if 1 <= num_workers <= 64:
                break
            print("Please enter a number between 1 and 64.")
        except ValueError:
            print("Invalid input. Please enter a number.")
    
    print()
    print(f"[INIT] Allocating system with {num_workers} workers...")
    
    #
    #
    #
    #
    #D E BU GING SECTION
    #
    #
    #
    DEBUG_TASK_COUNT = 950
    DEBUG_TASK_DELAY = 0.2     #0.2 seconds per task
    
    print(f"[DEBUG] Task delay: {DEBUG_TASK_DELAY}s per task")
    print(f"[DEBUG] Enqueuing {DEBUG_TASK_COUNT} flush tasks...")
    
    #Allocate with debug delay
    shared_queue, status_shm, processes, log_queue, lock, batch_lock = allocate_system(
        num_workers=num_workers,
        queue_slots=1024,
        slot_class=TaskSlot128,
        queue_name="main_queue",
        debug_task_delay=DEBUG_TASK_DELAY
    )
    
    for i in range(DEBUG_TASK_COUNT):
        t = TaskSlot128()
        t.tsk_id = i + 1
        t.fn_id = ProcTaskFnID.INCREMENT
        shared_queue.enqueue(t)
    
    print(f"[DEBUG] Enqueued {DEBUG_TASK_COUNT} tasks. Queue: {shared_queue.get_actual_occupancy()}")
    #######################################################################
    
    time.sleep(1)
    
    #Setup display
    display = SupervisorDisplay(num_workers=num_workers, queue_id=1)
    display.init_display()
    
    #Get status array view
    StatusArray = WorkerStatusStruct * 64
    status_array = StatusArray.from_buffer(status_shm.buf)
    
    #Start workers
    start_workers(processes)
    
    try:
        running = True
        while running:
            #Update display from status array
            total_completed = 0
            all_terminated = True
            
            for i in range(num_workers):
                status = status_array[i]
                display.update_worker(
                    worker_id=i,
                    state=status.state,
                    local_count=status.local_queue_count,
                    completed=status.completed_tasks
                )
                total_completed += status.completed_tasks
                if status.state != STATE_TERMINATED:
                    all_terminated = False
            
            #Update footer
            queue_occ = shared_queue.get_actual_occupancy()
            display.update_footer(queue_occ, total_completed)
            
            #Process log messages
            while not log_queue.empty():
                try:
                    worker_id, msg = log_queue.get_nowait()
                    display.add_log(worker_id, msg)
                except:
                    break
            
            #Check if all done
            if all_terminated:
                time.sleep(0.3)
                break
            
            time.sleep(0.05)
            
    except KeyboardInterrupt:
        pass
    
    #Cleanup
    display.finalize()
    print()
    print("[TERMINATION] Sending terminate signals to workers")
    stop_workers(shared_queue, num_workers)
    
    print("[TERMINATION] Waiting for workers...")
    join_workers(processes, timeout=3.0)
    
    #Final stats
    total_completed = sum(status_array[i].completed_tasks for i in range(num_workers))
    print()
    print("=" * 64)
    print("[TERMINATION] RESULT")
    print("=" * 64)
    for i in range(num_workers):
        status = status_array[i]
        state_name = STATE_NAMES.get(status.state, "UNKNOWN")
        print(f"Worker {i}: state={state_name}, completed={status.completed_tasks}")
    print(f"[TERMINATION] Total completed: {total_completed}")
    print(f"[TERMINATION] Expected: {DEBUG_TASK_COUNT}")
    print("=" * 64)
    
    #Cleanup shared memory
    print("Cleaning up shared memory...")
    cleanup_system(shared_queue, status_shm)
    print("Done.")


if __name__ == "__main__":
    main()