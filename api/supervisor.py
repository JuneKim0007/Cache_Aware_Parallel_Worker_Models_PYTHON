# ============================================================
# API/SUPERVISOR.PY
# ============================================================
# Supervisor display and control.
# ============================================================

import sys
import time
from multiprocessing.shared_memory import SharedMemory
from multiprocessing import Queue

from .queues import SharedTaskQueue
from .worker import (WorkerStatusStruct, STATE_NAMES, STATE_RUNNING,
                     STATE_IDLE, STATE_TERMINATED)


#============================================================
# TERMINAL CONTROL
#============================================================
def clear_screen():
    sys.stdout.write("\033[2J\033[H")
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


#============================================================
# SUPERVISOR DISPLAY
#============================================================
class SupervisorDisplay:
    def __init__(self, num_workers: int, queue_id: int = 1):
        self.num_workers = num_workers
        self.queue_id = queue_id
        self.worker_start_row = 5
        self.footer_row = self.worker_start_row + num_workers + 1
        self.log_start_row = self.footer_row + 2
        self.log_lines = []
        self.max_log_lines = 10
    
    def init_display(self):
        hide_cursor()
        clear_screen()
        
        move_cursor(1, 1)
        sys.stdout.write("MultiProcessing Task Supervisor")
        move_cursor(2, 1)
        sys.stdout.write("-" * 70)
        move_cursor(3, 1)
        sys.stdout.write(f"Queue ID: {self.queue_id}")
        move_cursor(4, 1)
        sys.stdout.write("-" * 70)
        
        move_cursor(self.worker_start_row, 1)
        sys.stdout.write(f"{'worker_id':<12}{'state':<15}{'local_queue':<15}{'completed':<15}")
        
        for i in range(self.num_workers):
            row = self.worker_start_row + 1 + i
            move_cursor(row, 1)
            sys.stdout.write(f"{i:<12}{'INIT':<15}{0:<15}{0:<15}")
        
        move_cursor(self.footer_row, 1)
        sys.stdout.write("-" * 70)
        move_cursor(self.footer_row + 1, 1)
        sys.stdout.write("Queue: 0     | Total: 0      | Ctrl+C to stop")
        
        move_cursor(self.log_start_row, 1)
        sys.stdout.write("-" * 70)
        move_cursor(self.log_start_row + 1, 1)
        sys.stdout.write("Log:")
        sys.stdout.flush()
    
    def update_worker(self, worker_id: int, state: int, local_count: int, completed: int):
        row = self.worker_start_row + 1 + worker_id
        state_name = STATE_NAMES.get(state, "UNKNOWN")
        move_cursor(row, 1)
        clear_line()
        sys.stdout.write(f"{worker_id:<12}{state_name:<15}{local_count:<15}{completed:<15}")
        sys.stdout.flush()
    
    def update_footer(self, queue_occupancy: int, total_completed: int, active_workers: int = 0):
        move_cursor(self.footer_row + 1, 1)
        clear_line()
        sys.stdout.write(f"Queue: {queue_occupancy:<5} | Total: {total_completed:<6} | "
                        f"Active: {active_workers:<3} | Ctrl+C to stop")
        sys.stdout.flush()
    
    def add_log(self, worker_id: int, message: str):
        prefix = "SYS" if worker_id < 0 else f"W{worker_id}"
        self.log_lines.append(f"[{prefix}] {message}")
        if len(self.log_lines) > self.max_log_lines:
            self.log_lines.pop(0)
        
        for i, line in enumerate(self.log_lines):
            row = self.log_start_row + 2 + i
            move_cursor(row, 1)
            clear_line()
            sys.stdout.write(line[:68])
        sys.stdout.flush()
    
    def finalize(self):
        move_cursor(self.log_start_row + 2 + self.max_log_lines + 1, 1)
        show_cursor()


#============================================================
# SUPERVISOR CONTROLLER
#============================================================
class SupervisorController:
    def __init__(self,
                 shared_queue: SharedTaskQueue,
                 status_shm: SharedMemory,
                 processes: list,
                 log_queue: Queue,
                 num_workers: int,
                 use_display: bool = True,
                 auto_terminate_on_empty: bool = True,
                 idle_check_interval: int = 10):
        '''
        Args:
            shared_queue: Shared task queue
            status_shm: Worker status shared memory
            processes: Worker processes
            log_queue: Log message queue
            num_workers: Number of workers
            use_display: Show terminal display
            auto_terminate_on_empty: If True, send TERMINATE when queue empty and workers idle
            idle_check_interval: Check termination condition every N poll cycles (performance)
        '''
        self.shared_queue = shared_queue
        self.status_shm = status_shm
        self.processes = processes
        self.log_queue = log_queue
        self.num_workers = num_workers
        self.use_display = use_display
        self.auto_terminate_on_empty = auto_terminate_on_empty
        self.idle_check_interval = idle_check_interval
        self.display = None
        self.running = False
        
        StatusArray = WorkerStatusStruct * 64
        self.status_array = StatusArray.from_buffer(status_shm.buf)
    
    def run(self, poll_interval: float = 0.05) -> int:
        exit_code = 0
        interrupted = False
        
        if self.use_display:
            self.display = SupervisorDisplay(self.num_workers, self.shared_queue.queue_id)
            self.display.init_display()
        
        self._start_workers()
        self.running = True
        idle_cycles = 0
        poll_count = 0
        
        try:
            while self.running:
                total_completed = 0
                all_terminated = True
                all_idle = True
                
                for i in range(self.num_workers):
                    status = self.status_array[i]
                    
                    if self.use_display and self.display:
                        self.display.update_worker(i, status.state, status.local_queue_count,
                                                  status.completed_tasks)
                    
                    total_completed += status.completed_tasks
                    if status.state != STATE_TERMINATED:
                        all_terminated = False
                    if status.state != STATE_IDLE and status.state != STATE_TERMINATED:
                        all_idle = False
                
                if self.use_display and self.display:
                    queue_occ = self.shared_queue.get_actual_occupancy()
                    active = self.shared_queue.get_active_worker_count()
                    self.display.update_footer(queue_occ, total_completed, active)
                
                self._process_logs()
                
                if all_terminated:
                    time.sleep(0.1)
                    break
                
                #Performance: only check termination condition every N cycles
                poll_count += 1
                if self.auto_terminate_on_empty and (poll_count % self.idle_check_interval == 0):
                    if all_idle:
                        queue_occ = self.shared_queue.get_actual_occupancy()
                        if queue_occ == 0:
                            idle_cycles += 1
                            if idle_cycles > 2:
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
                self.display.add_log(-1, "Ctrl+C received")
        
        finally:
            self.running = False
            
            if self.use_display and self.display:
                self.display.finalize()
            
            if interrupted:
                self._send_terminate_signals()
            
            self._join_workers()
            self._print_final_stats()
            self._cleanup()
        
        return exit_code
    
    def _start_workers(self):
        for p in self.processes:
            p.start()
    
    def _send_terminate_signals(self):
        from .slot import TaskSlot128, ProcTaskFnID
        for i in range(self.num_workers):
            term = TaskSlot128()
            term.tsk_id = 0xFFFF + i
            term.fn_id = ProcTaskFnID.TERMINATE
            self.shared_queue.enqueue(term)
    
    def _join_workers(self, timeout: float = 3.0):
        print("Waiting for workers to terminate...")
        for p in self.processes:
            p.join(timeout=timeout)
            if p.is_alive():
                p.terminate()
    
    def _process_logs(self):
        while not self.log_queue.empty():
            try:
                worker_id, msg = self.log_queue.get_nowait()
                if self.display:
                    self.display.add_log(worker_id, msg)
            except:
                break
    
    def _print_final_stats(self):
        stats = self.get_stats()
        print()
        print("=" * 50)
        print("FINAL RESULTS")
        print("=" * 50)
        for w in stats['workers']:
            print(f"Worker {w['worker_id']}: state={w['state']}, completed={w['completed']}")
        print("-" * 50)
        print(f"Total completed: {stats['total_completed']}")
        print(f"Debug counter:   {stats['debug_counter']}")
        print("=" * 50)
    
    def _cleanup(self):
        print()
        print("Cleaning up...")
        from .allocation import cleanup
        cleanup(self.shared_queue, self.status_shm)
        print("Done.")
    
    def get_stats(self) -> dict:
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
            'debug_counter': self.shared_queue.get_debug_counter(),
            'workers': worker_stats
        }