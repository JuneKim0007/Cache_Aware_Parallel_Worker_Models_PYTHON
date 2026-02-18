

import sys
import time
from multiprocessing.shared_memory import SharedMemory

from .config import SyncGroup, SupervisorConfig
from .queues import SharedTaskQueue
from .worker import (WorkerStatusStruct, STATE_NAMES, STATE_RUNNING,
                     STATE_IDLE, STATE_TERMINATED,
                     STATUS_ARRAY_TYPE)


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


### USER FACING DISPLAY
class _Display:
    '''
    TTY display for worker status. Internal use only.
    ''' 
    def __init__(self, num_workers: int, queue_id: int, max_logs: int = 8):
        self.num_workers = num_workers
        self.queue_id = queue_id
        self.max_logs = max_logs

        self.title_row = 1
        self.worker_header_row = 2
        self.worker_start_row = 3
        self.footer_sep_row = self.worker_start_row + num_workers
        self.footer_row = self.footer_sep_row + 1
        self.log_label_row = self.footer_row + 2
        self.log_start_row = self.log_label_row + 1
        self.final_row = self.log_start_row + max_logs + 1

        self.logs = []

        _hide_cursor()
        _clear_screen()

        _move_cursor(self.title_row, 1)
        print("MultiProcessing Task Supervisor")
        print("-" * 70)
        print(f"Queue ID: {self.queue_id}")
        print("-" * 70)

        _move_cursor(self.worker_header_row, 1)
        print(f"{'worker_id':<12}{'state':<15}{'local_queue':<15}{'completed':<15}")

        _move_cursor(self.footer_sep_row, 1)
        print("-" * 70)
        _move_cursor(self.footer_row, 1)
        print(f"Queue: {0:<5} | Total: {0:<6} | Active: {0:<4} | Ctrl+C to stop")

        _move_cursor(self.log_label_row, 1)
        print("-" * 70)
        print("Log:")

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
        prefix = f"[W{wid}]" if wid >= 0 else "[SYS]"
        entry = f"{prefix} {msg}"

        self.logs.append(entry)
        if len(self.logs) > self.max_logs:
            self.logs.pop(0)

        for i in range(self.max_logs):
            _move_cursor(self.log_start_row + i, 1)
            _clear_line()
            if i < len(self.logs):
                sys.stdout.write(self.logs[i][:68])
        sys.stdout.flush()

    def finalize(self):
        _move_cursor(self.final_row, 1)
        _show_cursor()


class SupervisorController:

    def __init__(self,
                 shared_queue: SharedTaskQueue,
                 status_shm: SharedMemory,
                 processes: list,
                 sync: SyncGroup,
                 num_workers: int,
                 config: SupervisorConfig):

        self.shared_queue = shared_queue
        self.status_shm = status_shm
        self.processes = processes
        self.log_queue = sync.log_queue
        self.num_workers = num_workers
        self._cfg = config

        self._display = None
        self._running = False

        self._status_array = STATUS_ARRAY_TYPE.from_buffer(status_shm.buf)

    def run(self, enqueue_callback=None) -> int:

        print("[WORKERS INITIALIZING]")
        for i, p in enumerate(self.processes):
            p.start()
            print(f"  Worker [{i}] started")
        print(f"[ALL {self.num_workers} WORKERS STARTED]")
        print()
        self._running = True

        cfg = self._cfg
        if cfg.display:
            self._display = _Display(self.num_workers, self.shared_queue.queue_id)

        exit_code = 0
        interrupted = False
        idle_cycles = 0
        poll_count = 0
        enqueue_done = False

        try:
            while self._running:
                result = self._poll()

                if enqueue_callback and not enqueue_done:
                    try:
                        if enqueue_callback() == False:
                            enqueue_done = True
                    except StopIteration:
                        enqueue_done = True

                if result['all_terminated']:
                    time.sleep(0.1)
                    break

                ######Auto terminate when idle
                poll_count += 1
                if cfg.auto_terminate and (poll_count % cfg.idle_check_interval == 0):
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

                time.sleep(cfg.poll_interval)

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

        while not self.log_queue.empty():
            try:
                wid, msg = self.log_queue.get_nowait()
                if self._display:
                    self._display.add_log(wid, msg)
            except:
                break

        return {'all_idle': all_idle, 'all_terminated': all_terminated, 'total': total}

    def _send_terminate(self):
        from .slot import ProcTaskFnID
        slot_cfg = self.shared_queue.slot_cfg
        for i in range(self.num_workers):
            t = slot_cfg.build_slot(tsk_id=0xFFFF + i, fn_id=ProcTaskFnID.TERMINATE)
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
            total += s.completed_tasks
            if not self._cfg.display:
                name = STATE_NAMES.get(s.state, "?")
                print(f"Worker {i}: state={name}, completed={s.completed_tasks}")
        if not self._cfg.display:
            print("-" * 50)
        print(f"Total completed: {total}")
        print(f"Debug counter:   {self.shared_queue.get_debug_counter()}")
        print("=" * 50)

    def _cleanup(self):
        print()
        print("Cleaning up...")

        del self._status_array
        time.sleep(0.1) #YIELD TEMPORARLY

        from .allocation import cleanup
        cleanup(self.shared_queue, self.status_shm)

        print("Done.")