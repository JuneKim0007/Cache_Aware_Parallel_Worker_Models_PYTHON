# ============================================================
# API/SUPERVISOR.PY — Supervisor process controller
# ============================================================
# SharedMemory ownership: Supervisor OWNS all segments.
#   1. _join_workers()  — all workers exit (they close their handles)
#   2. _print_stats()   — read final status (views still alive)
#   3. _cleanup()       — del views → gc → close → unlink
# ============================================================

import gc
import sys
import time
from multiprocessing.shared_memory import SharedMemory
from multiprocessing import Queue

from .queues import SharedTaskQueue
from .worker import (WorkerStatusStruct, STATE_NAMES, STATE_RUNNING,
                     STATE_IDLE, STATE_TERMINATED)


# ============================================================
# TERMINAL CONTROL
# ============================================================
def _clear_screen():
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()

def _move_cursor(row, col):
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


# ============================================================
# DISPLAY
# ============================================================
class _Display:
    def __init__(self, num_workers, queue_id, max_logs=8):
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
        print("Mpop Task Supervisor")
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

    def update_worker(self, wid, state_val, local, done):
        _move_cursor(self.worker_start_row + wid, 1)
        _clear_line()
        sys.stdout.write(f"{wid:<12}{STATE_NAMES.get(state_val, '?'):<15}{local:<15}{done:<15}")
        sys.stdout.flush()

    def update_footer(self, queue_occ, total, active):
        _move_cursor(self.footer_row, 1)
        _clear_line()
        sys.stdout.write(f"Queue: {queue_occ:<5} | Total: {total:<6} | Active: {active:<4} | Ctrl+C to stop")
        sys.stdout.flush()

    def add_log(self, wid, msg):
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


# ============================================================
# SUPERVISOR CONTROLLER
# ============================================================
class SupervisorController:

    def __init__(self, shared_queue, status_shm, processes, log_queue,
                 num_workers, display=True, auto_terminate=True,
                 poll_interval=0.05, idle_check_interval=10):
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

    def run(self, enqueue_callback=None):
        print("[WORKERS INITIALIZING]")
        for i, p in enumerate(self.processes):
            p.start()
            print(f"  Worker [{i}] started")
        print(f"[ALL {self.num_workers} WORKERS STARTED]")
        print()
        self._running = True

        if self._display_enabled:
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

            # ORDER MATTERS — do not rearrange:
            # 1. Join workers (they release views + close handles)
            self._join_workers()
            # 2. Read final stats (supervisor views still alive)
            self._print_stats()
            # 3. Release supervisor views → gc → close → unlink
            self._cleanup()

        return exit_code

    def _poll(self):
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
        from .slot import TaskSlot, ProcTaskFnID
        for i in range(self.num_workers):
            t = TaskSlot()
            t.tsk_id = 0xFFFF + i
            t.fn_id = ProcTaskFnID.TERMINATE
            self.shared_queue.enqueue(t)

    def _join_workers(self, timeout=5.0):
        """Wait for all workers to exit. Workers close() their shm handles."""
        print("Waiting for workers to terminate...")
        for p in self.processes:
            p.join(timeout=timeout)
            if p.is_alive():
                p.terminate()
                p.join(timeout=2.0)

    def _print_stats(self):
        """Read final stats. Views are still alive — not yet released."""
        print()
        print("=" * 50)
        print("FINAL RESULTS")
        print("=" * 50)
        total = 0
        for i in range(self.num_workers):
            s = self._status_array[i]
            total += s.completed_tasks
            if not self._display_enabled:
                name = STATE_NAMES.get(s.state, "?")
                print(f"Worker {i}: state={name}, completed={s.completed_tasks}")
        if not self._display_enabled:
            print("-" * 50)
        print(f"Total completed: {total}")
        print(f"Debug counter:   {self.shared_queue.get_debug_counter()}")
        print("=" * 50)

    def _cleanup(self):
        """Release all SharedMemory. ONLY called after all workers joined.
        
        Order: del views → gc → close → unlink
        Supervisor OWNS these segments, so unlink() handles resource_tracker.
        """
        print()
        print("Cleaning up...")

        # Phase 1: Release all ctypes views referencing shm buffers
        del self._status_array
        self.shared_queue.release_views()

        # drop internal buffer references while workers are terminating.
        gc.collect()

        # Phase 3: Close + unlink queue shm
        self.shared_queue.close()
        self.shared_queue.unlink()

        # Phase 4: Close + unlink status shm
        try:
            self.status_shm.close()
        except Exception:
            pass
        try:
            self.status_shm.unlink()
        except Exception:
            pass

        print("Done.")