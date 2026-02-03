# ============================================================
# SUPERVISOR.PY
# ============================================================
# Supervisor display and control logic for multiprocessing task system.
# Handles terminal UI, worker status monitoring, and graceful shutdown.
# ============================================================

import os
import sys
import time
from multiprocessing.shared_memory import SharedMemory
from multiprocessing import Queue

from queues import SharedTaskQueue
from worker import (WorkerStatusStruct, STATE_NAMES, STATE_RUNNING, 
                    STATE_IDLE, STATE_TERMINATED)


#============================================================
# TERMINAL CONTROL
#============================================================
def clear_screen():
    sys.stdout.write("\033[2J")
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

def get_terminal_size():
    '''Get terminal size (columns, rows)'''
    import shutil
    size = shutil.get_terminal_size()
    return size.columns, size.lines


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
    Row 5: Worker column headers
    Row 6+: Worker status (one per worker)
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
        
        #Check terminal size
        cols, rows = get_terminal_size()
        min_rows = self.log_start_row + 2 + self.max_log_lines
        if rows < min_rows:
            print(f"[Warning] Terminal has {rows} rows, recommend {min_rows}+ for {num_workers} workers")
    
    def init_display(self):
        '''Initialize the display layout'''
        hide_cursor()
        clear_screen()
        
        #Header
        move_cursor(1, 1)
        sys.stdout.write("MultiProcessing Task Supervisor")
        move_cursor(2, 1)
        sys.stdout.write("-" * 70)
        move_cursor(3, 1)
        sys.stdout.write(f"Queue ID: {self.queue_id} | PID: {os.getpid()}")
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
        sys.stdout.write("Log:")
        
        sys.stdout.flush()
    
    def update_worker(self, worker_id: int, state: int, local_count: int, completed: int):
        '''Update a single worker's status line'''
        row = self.worker_start_row + 1 + worker_id
        state_name = STATE_NAMES.get(state, "UNKNOWN")
        
        move_cursor(row, 1)
        clear_line()
        sys.stdout.write(f"{worker_id:<12}{state_name:<15}{local_count:<15}{completed:<15}")
        sys.stdout.flush()
    
    def update_footer(self, queue_occupancy: int, total_completed: int, active_workers: int = 0):
        '''Update footer with queue and total stats'''
        move_cursor(self.footer_row + 1, 1)
        clear_line()
        sys.stdout.write(f"Queue: {queue_occupancy:<5} | Total: {total_completed:<6} | "
                        f"Active: {active_workers:<3} | Ctrl+C to stop")
        sys.stdout.flush()
    
    def add_log(self, worker_id: int, message: str):
        '''Add a log entry (scrolling area)'''
        prefix = "SYS" if worker_id < 0 else f"W{worker_id}"
        self.log_lines.append(f"[{prefix}] {message}")
        if len(self.log_lines) > self.max_log_lines:
            self.log_lines.pop(0)
        
        #Redraw log area
        for i, line in enumerate(self.log_lines):
            row = self.log_start_row + 2 + i
            move_cursor(row, 1)
            clear_line()
            sys.stdout.write(line[:68])
        
        sys.stdout.flush()
    
    def finalize(self):
        '''Restore terminal state'''
        move_cursor(self.log_start_row + 2 + self.max_log_lines + 1, 1)
        show_cursor()


#============================================================
# SUPERVISOR CONTROLLER
#============================================================
class SupervisorController:
    '''
    Controls the supervisor loop: monitors workers, processes logs, handles shutdown.
    
    Usage:
        supervisor = SupervisorController(...)
        supervisor.run()  # Handles everything, returns exit code
    '''
    
    def __init__(self,
                 shared_queue: SharedTaskQueue,
                 status_shm: SharedMemory,
                 processes: list,
                 log_queue: Queue,
                 num_workers: int,
                 use_display: bool = True):
        self.shared_queue = shared_queue
        self.status_shm = status_shm
        self.processes = processes
        self.log_queue = log_queue
        self.num_workers = num_workers
        self.use_display = use_display
        self.display = None
        self.running = False
        
        #Get status array view
        StatusArray = WorkerStatusStruct * 64
        self.status_array = StatusArray.from_buffer(status_shm.buf)
    
    def run(self, poll_interval: float = 0.05, auto_terminate: bool = True) -> int:
        '''
        Main supervisor loop. Handles everything:
        1. Initialize display
        2. Start workers
        3. Monitor until completion or Ctrl+C
        4. Stop workers (send terminate signals)
        5. Print final stats
        6. Cleanup
        
        Args:
            poll_interval: How often to poll status (seconds)
            auto_terminate: If True, send TERMINATE signals when queue empty
            
        Returns:
            0 on success, 1 on error/interrupt
        '''
        exit_code = 0
        interrupted = False
        
        #Initialize display
        if self.use_display:
            self.display = SupervisorDisplay(
                num_workers=self.num_workers,
                queue_id=self.shared_queue.queue_id
            )
            self.display.init_display()
        
        #Start workers
        self._start_workers()
        
        self.running = True
        idle_cycles = 0
        
        try:
            while self.running:
                #Update worker status
                total_completed = 0
                all_terminated = True
                all_idle = True
                
                for i in range(self.num_workers):
                    status = self.status_array[i]
                    
                    if self.use_display and self.display:
                        self.display.update_worker(
                            worker_id=i,
                            state=status.state,
                            local_count=status.local_queue_count,
                            completed=status.completed_tasks
                        )
                    
                    total_completed += status.completed_tasks
                    if status.state != STATE_TERMINATED:
                        all_terminated = False
                    if status.state != STATE_IDLE and status.state != STATE_TERMINATED:
                        all_idle = False
                
                #Update footer
                if self.use_display and self.display:
                    queue_occ = self.shared_queue.get_actual_occupancy()
                    active = self.shared_queue.get_active_worker_count()
                    self.display.update_footer(queue_occ, total_completed, active)
                
                #Process log messages
                self._process_logs()
                
                #Check termination
                if all_terminated:
                    time.sleep(0.1)
                    break
                
                #Auto-terminate when queue empty and all workers idle
                if auto_terminate and all_idle:
                    queue_occ = self.shared_queue.get_actual_occupancy()
                    if queue_occ == 0:
                        idle_cycles += 1
                        if idle_cycles > 10:  #Wait a bit to be sure
                            self._send_terminate_signals()
                            idle_cycles = 0
                    else:
                        idle_cycles = 0
                else:
                    idle_cycles = 0
                
                time.sleep(poll_interval)
                
        except KeyboardInterrupt:
            interrupted = True
            exit_code = 1
            if self.use_display and self.display:
                self.display.add_log(-1, "Ctrl+C received, shutting down...")
        
        finally:
            self.running = False
            
            #Finalize display first
            if self.use_display and self.display:
                self.display.finalize()
            
            #Send terminate signals if interrupted
            if interrupted:
                self._send_terminate_signals()
            
            #Wait for workers
            self._join_workers()
            
            #Print final stats
            self._print_final_stats()
            
            #Cleanup
            self._cleanup()
        
        return exit_code
    
    def _start_workers(self):
        '''Start all worker processes'''
        for p in self.processes:
            p.start()
    
    def _send_terminate_signals(self):
        '''Send TERMINATE signals to all workers'''
        from slot import TaskSlot128, ProcTaskFnID
        
        for i in range(self.num_workers):
            term = TaskSlot128()
            term.tsk_id = 0xFFFF + i
            term.fn_id = ProcTaskFnID.TERMINATE
            self.shared_queue.enqueue(term)
    
    def _join_workers(self, timeout: float = 3.0):
        '''Wait for workers to terminate'''
        print("Waiting for workers to terminate...")
        for p in self.processes:
            p.join(timeout=timeout)
            if p.is_alive():
                p.terminate()
    
    def _process_logs(self):
        '''Process pending log messages'''
        while not self.log_queue.empty():
            try:
                worker_id, msg = self.log_queue.get_nowait()
                if self.display:
                    self.display.add_log(worker_id, msg)
            except:
                break
    
    def _print_final_stats(self):
        '''Print final statistics'''
        stats = self.get_stats()
        
        print()
        print("=" * 50)
        print("FINAL RESULTS")
        print("=" * 50)
        for w in stats['workers']:
            print(f"Worker {w['worker_id']}: state={w['state']}, completed={w['completed']}")
        print("-" * 50)
        print(f"Total completed: {stats['total_completed']}")
        print("=" * 50)
    
    def _cleanup(self):
        '''Cleanup shared memory'''
        print()
        print("Cleaning up...")
        
        from allocation import cleanup
        cleanup(self.shared_queue, self.status_shm)
        
        print("Done.")
    
    def get_stats(self) -> dict:
        '''Get current statistics'''
        total_completed = 0
        worker_stats = []
        
        for i in range(self.num_workers):
            status = self.status_array[i]
            total_completed += status.completed_tasks
            worker_stats.append({
                'worker_id': i,
                'state': STATE_NAMES.get(status.state, "UNKNOWN"),
                'completed': status.completed_tasks
            })
        
        return {
            'total_completed': total_completed,
            'queue_occupancy': self.shared_queue.get_actual_occupancy(),
            'active_workers': self.shared_queue.get_active_worker_count(),
            'workers': worker_stats
        }