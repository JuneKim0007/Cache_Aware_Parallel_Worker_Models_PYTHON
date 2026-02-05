# ============================================================
# API/SUPERVISOR.PY
# ============================================================
# Single central flow: run() handles everything.
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
def _clear_screen():
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()

def _move_cursor(row: int, col: int):
    sys.stdout.write(f"\033[{row};{col}H")
    sys.stdout.flush()

def _clear_line():
    sys.stdout.write("\033[2K")
    sys.stdout.flush()

def _hide_cursor():
    sys.stdout.write("\033[?25l")
    sys.stdout.flush()

def _show_cursor():
    sys.stdout.write("\033[?25h")
    sys.stdout.flush()


#============================================================
# SUPERVISOR DISPLAY
#============================================================
class _Display:
    '''
    TTY display for worker status. Internal use only.
    
    Layout (fixed rows, no scrolling):
        Row 1:    Title
        Row 2:    Separator
        Row 3:    Queue ID
        Row 4:    Separator
        Row 5:    Worker header
        Row 6-N:  Workers (N = num_workers)
        Row N+1:  Separator
        Row N+2:  Footer (queue stats)
        Row N+3:  Separator
        Row N+4:  "Log:" label
        Row N+5 to N+5+max_logs: Log lines (fixed, ring buffer)
        Row N+5+max_logs+1: Final output row
    '''
    
    def __init__(self, num_workers: int, queue_id: int, max_logs: int = 8):
        self.num_workers = num_workers
        self.queue_id = queue_id
        self.max_logs = max_logs
        
        # Calculate fixed row positions
        self.title_row = 1
        self.worker_header_row = 5
        self.worker_start_row = 6
        self.footer_sep_row = self.worker_start_row + num_workers
        self.footer_row = self.footer_sep_row + 1
        self.log_label_row = self.footer_row + 2
        self.log_start_row = self.log_label_row + 1
        self.final_row = self.log_start_row + max_logs + 1
        
        # Ring buffer for logs
        self.logs = []
    
    def init(self):
        _hide_cursor()
        _clear_screen()
        
        # Title section
        _move_cursor(self.title_row, 1)
        print("MultiProcessing Task Supervisor")
        print("-" * 70)
        print(f"Queue ID: {self.queue_id}")
        print("-" * 70)
        
        # Worker header
        _move_cursor(self.worker_header_row, 1)
        print(f"{'worker_id':<12}{'state':<15}{'local_queue':<15}{'completed':<15}")
        
        # Worker rows (init)
        for i in range(self.num_workers):
            _move_cursor(self.worker_start_row + i, 1)
            print(f"{i:<12}{'INIT':<15}{0:<15}{0:<15}")
        
        # Footer section
        _move_cursor(self.footer_sep_row, 1)
        print("-" * 70)
        _move_cursor(self.footer_row, 1)
        print(f"Queue: {0:<5} | Total: {0:<6} | Active: {0:<4} | Ctrl+C to stop")
        
        # Log section
        _move_cursor(self.log_label_row, 1)
        print("-" * 70)
        print("Log:")
        
        # Pre-clear all log lines (fixed area)
        for i in range(self.max_logs):
            _move_cursor(self.log_start_row + i, 1)
            _clear_line()
    
    def update_worker(self, wid: int, state: int, local: int, done: int):
        _move_cursor(self.worker_start_row + wid, 1)
        _clear_line()
        sys.stdout.write(f"{wid:<12}{STATE_NAMES.get(state, '?'):<15}{local:<15}{done:<15}")
        sys.stdout.flush()
    
    def update_footer(self, queue_occ: int, total: int, active: int):
        _move_cursor(self.footer_row, 1)
        _clear_line()
        sys.stdout.write(f"Queue: {queue_occ:<5} | Total: {total:<6} | Active: {active:<4} | Ctrl+C to stop")
        sys.stdout.flush()
    
    def add_log(self, wid: int, msg: str):
        '''Add log to ring buffer and update fixed log area.'''
        prefix = f"[W{wid}]" if wid >= 0 else "[SYS]"
        entry = f"{prefix} {msg}"
        
        self.logs.append(entry)
        if len(self.logs) > self.max_logs:
            self.logs.pop(0)
        
        # Redraw all log lines in fixed area
        for i in range(self.max_logs):
            _move_cursor(self.log_start_row + i, 1)
            _clear_line()
            if i < len(self.logs):
                sys.stdout.write(self.logs[i][:68])
        sys.stdout.flush()
    
    def finalize(self):
        '''Move cursor below display for final output.'''
        _move_cursor(self.final_row, 1)
        _show_cursor()


#============================================================
# SUPERVISOR CONTROLLER
#============================================================
class SupervisorController:
    '''
    Internal supervisor. All config in __init__, run() is the only method.
    '''
    
    def __init__(self,
                 shared_queue: SharedTaskQueue,
                 status_shm: SharedMemory,
                 processes: list,
                 log_queue: Queue,
                 num_workers: int,
                 display: bool = True,
                 auto_terminate: bool = True,
                 poll_interval: float = 0.05,
                 idle_check_interval: int = 10):
        
        self.shared_queue = shared_queue
        self.status_shm = status_shm
        self.processes = processes
        self.log_queue = log_queue
        self.num_workers = num_workers
        self._display_enabled = display
        self._auto_terminate = auto_terminate
        self._poll_interval = poll_interval
        self._idle_check_interval = idle_check_interval
        
        self._display = None
        self._running = False
        
        StatusArray = WorkerStatusStruct * 64
        self._status_array = StatusArray.from_buffer(status_shm.buf)
    
    def run(self, enqueue_callback=None) -> int:
        '''
        Run until completion. Single user-facing method.
        
        Args:
            enqueue_callback: Optional func() called each loop for dynamic enqueue.
                             Return False or raise StopIteration to stop calling.
        
        Returns:
            Exit code (0=success, 1=interrupted)
        '''
        # Init display
        if self._display_enabled:
            self._display = _Display(self.num_workers, self.shared_queue.queue_id)
            self._display.init()
        
        # Start workers
        for p in self.processes:
            p.start()
        self._running = True
        
        exit_code = 0
        interrupted = False
        idle_cycles = 0
        poll_count = 0
        enqueue_done = False
        
        try:
            while self._running:
                # Poll status and update display
                result = self._poll()
                
                # Dynamic enqueue callback
                if enqueue_callback and not enqueue_done:
                    try:
                        if enqueue_callback() == False:
                            enqueue_done = True
                    except StopIteration:
                        enqueue_done = True
                
                # Check all terminated
                if result['all_terminated']:
                    time.sleep(0.1)
                    break
                
                # Auto terminate when idle
                poll_count += 1
                if self._auto_terminate and (poll_count % self._idle_check_interval == 0):
                    if result['all_idle']:
                        if self.shared_queue.get_actual_occupancy() == 0:
                            idle_cycles += 1
                            if idle_cycles > 2:
                                self._send_terminate()
                                idle_cycles = 0
                        else:
                            idle_cycles = 0
                    else:
                        idle_cycles = 0
                
                time.sleep(self._poll_interval)
                
        except KeyboardInterrupt:
            interrupted = True
            exit_code = 1
            if self._display:
                self._display.add_log(-1, "Ctrl+C received")
        
        finally:
            self._running = False
            if self._display:
                self._display.finalize()
            if interrupted:
                self._send_terminate()
            self._join_workers()
            self._print_stats()
            self._cleanup()
        
        return exit_code
    
    def _poll(self) -> dict:
        '''Update display and return status.'''
        total = 0
        all_terminated = True
        all_idle = True
        
        for i in range(self.num_workers):
            s = self._status_array[i]
            if self._display:
                self._display.update_worker(i, s.state, s.local_queue_count, s.completed_tasks)
            total += s.completed_tasks
            if s.state != STATE_TERMINATED:
                all_terminated = False
            if s.state not in (STATE_IDLE, STATE_TERMINATED):
                all_idle = False
        
        if self._display:
            occ = self.shared_queue.get_actual_occupancy()
            active = self.shared_queue.get_active_worker_count()
            self._display.update_footer(occ, total, active)
        
        # Process log queue
        while not self.log_queue.empty():
            try:
                wid, msg = self.log_queue.get_nowait()
                if self._display:
                    self._display.add_log(wid, msg)
            except:
                break
        
        return {'all_idle': all_idle, 'all_terminated': all_terminated, 'total': total}
    
    def _send_terminate(self):
        from .slot import TaskSlot128, ProcTaskFnID
        for i in range(self.num_workers):
            t = TaskSlot128()
            t.tsk_id = 0xFFFF + i
            t.fn_id = ProcTaskFnID.TERMINATE
            self.shared_queue.enqueue(t)
    
    def _join_workers(self, timeout: float = 3.0):
        print("Waiting for workers to terminate...")
        for p in self.processes:
            p.join(timeout=timeout)
            if p.is_alive():
                p.terminate()
                p.join(timeout=1.0)
    
    def _print_stats(self):
        print()
        print("=" * 50)
        print("FINAL RESULTS")
        print("=" * 50)
        total = 0
        for i in range(self.num_workers):
            s = self._status_array[i]
            name = STATE_NAMES.get(s.state, "?")
            print(f"Worker {i}: state={name}, completed={s.completed_tasks}")
            total += s.completed_tasks
        print("-" * 50)
        print(f"Total completed: {total}")
        print(f"Debug counter:   {self.shared_queue.get_debug_counter()}")
        print("=" * 50)
    
    def _cleanup(self):
        print()
        print("Cleaning up...")
        
        # Delete local references to shared memory views
        del self._status_array
        
        # Small delay to ensure worker processes have fully exited
        time.sleep(0.1)
        
        # Now safe to cleanup
        self.shared_queue.cleanup()
        try:
            self.status_shm.close()
            self.status_shm.unlink()
        except Exception:
            pass
        
        print("Done.")