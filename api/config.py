# ============================================================
# API/CONFIG.PY
# ============================================================
# Central configuration + front-end validation.
#
# MpopConfig absorbs ALL user-facing parameters.
# Each sub-config validates itself.  MpopConfig.validate()
# aggregates everything into a ValidationResult that can be
# printed BEFORE any allocation happens.
#
# No downstream module should recompute anything that's here.
# ============================================================

import ctypes
import struct
from multiprocessing import Lock, Queue
from typing import Type, List


# ============================================================
# VALIDATION
# ============================================================
_PASS = "pass"
_WARN = "warn"
_FAIL = "fail"

_SYMBOLS = {_PASS: "OK", _WARN: "!!", _FAIL: "XX"}


class ValidationNote:
    """Single validation finding."""
    __slots__ = ("level", "component", "message")

    def __init__(self, level: str, component: str, message: str):
        self.level = level
        self.component = component
        self.message = message

    def __repr__(self):
        return f"[{_SYMBOLS[self.level]}][{self.component}] {self.message}"


class ValidationResult:
    """Aggregated validation findings."""
    __slots__ = ("notes",)

    def __init__(self):
        self.notes: List[ValidationNote] = []

    def add(self, level: str, component: str, message: str):
        self.notes.append(ValidationNote(level, component, message))

    def ok(self, component: str, message: str):
        self.add(_PASS, component, message)

    def warn(self, component: str, message: str):
        self.add(_WARN, component, message)

    def fail(self, component: str, message: str):
        self.add(_FAIL, component, message)

    def merge(self, other: 'ValidationResult'):
        self.notes.extend(other.notes)

    @property
    def passed(self) -> bool:
        return not any(n.level == _FAIL for n in self.notes)

    @property
    def error_count(self) -> int:
        return sum(1 for n in self.notes if n.level == _FAIL)

    @property
    def warn_count(self) -> int:
        return sum(1 for n in self.notes if n.level == _WARN)

    def print_report(self):
        """Print formatted validation report."""
        w = 58
        print("=" * w)
        print("MPOP CONFIGURATION VALIDATION".center(w))
        print("=" * w)

        # Group by component
        components = []
        seen = set()
        for n in self.notes:
            if n.component not in seen:
                components.append(n.component)
                seen.add(n.component)

        for comp in components:
            print(f"  {comp}")
            for n in self.notes:
                if n.component == comp:
                    sym = _SYMBOLS[n.level]
                    print(f"    [{sym}] {n.message}")
            print()

        print("-" * w)
        status = "PASSED" if self.passed else "FAILED"
        print(f"  Result: {status}  "
              f"({self.error_count} errors, {self.warn_count} warnings)")
        print("=" * w)


# ============================================================
# SLOT CONFIG
# ============================================================
class SlotConfig:
    """Slot type metadata.  Computed once from the ctypes Structure
    class, then shared by queues, workers, allocation, and the API.
    """
    __slots__ = ("cls", "cls_name", "size", "int_args_cap", "c_args_cap",
                 "typed_args_cap", "is_typed")

    def __init__(self, slot_class: Type[ctypes.Structure]):
        from .slot import has_char_args, is_typed_slot
        self.cls = slot_class
        self.cls_name = slot_class.__name__
        self.size = ctypes.sizeof(slot_class)
        self.int_args_cap = 0
        self.c_args_cap = 0
        self.typed_args_cap = 0
        self.is_typed = is_typed_slot(slot_class)

        for fname, ftype, *_ in slot_class._fields_:
            if fname == 'args':
                if self.is_typed:
                    # Universal slot: args is uint8 array, 8 bytes per cell
                    self.typed_args_cap = ftype._length_ // 8
                else:
                    self.int_args_cap = ftype._length_
            elif fname == 'c_args' and has_char_args(slot_class):
                self.c_args_cap = ftype._length_

    @property
    def has_c_args(self) -> bool:
        return self.c_args_cap > 0

    @property
    def args_cap(self) -> int:
        """Effective arg capacity (typed or int)."""
        return self.typed_args_cap if self.is_typed else self.int_args_cap

    def make_array(self, count: int):
        """Return a ctypes Array type: slot_class * count."""
        return self.cls * count

    def build_slot(self, tsk_id: int = 0, fn_id: int = 0,
                   args: tuple = (), c_args: bytes = b'',
                   pool_id: int = 0, arg_spec=None):
        """Construct a fully populated slot instance.

        For typed slots (TaskSlot):
          If arg_spec provided → pack via type system (struct.pack_into).
          If no arg_spec → default to int64 for each arg.

        For legacy slots:
          Pack int args directly, c_args by truncation.
        """
        slot = self.cls()
        slot.tsk_id = tsk_id
        slot.fn_id = fn_id

        if self.is_typed:
            # ---- Universal typed slot ----
            from .types import pack_slot_args, default_arg_spec
            if args:
                spec = arg_spec or default_arg_spec(len(args))
                pack_slot_args(slot, spec, args)
        else:
            # ---- Legacy slot ----
            cap = self.int_args_cap
            for i in range(len(args)):
                if i >= cap:
                    break
                slot.args[i] = args[i]

            if pool_id > 0:
                struct.pack_into('<I', slot.meta, 0, pool_id)

            if c_args and self.c_args_cap > 0:
                slot.c_args = c_args[:self.c_args_cap]

        return slot

    def validate(self) -> ValidationResult:
        """Validate slot configuration."""
        r = ValidationResult()
        comp = "Slot"

        from .slot import SLOT_REGISTRY
        if self.cls in SLOT_REGISTRY:
            r.ok(comp, f"{self.cls_name} ({self.size} bytes) registered")
        else:
            r.fail(comp, f"{self.cls_name} not in SLOT_REGISTRY")

        if self.is_typed:
            r.ok(comp, f"universal typed slot: {self.typed_args_cap} arg cells")
        elif self.int_args_cap > 0:
            r.ok(comp, f"int_args capacity: {self.int_args_cap}")
        else:
            r.warn(comp, "int_args capacity is 0 (no integer arguments)")

        if self.has_c_args:
            r.ok(comp, f"c_args capacity: {self.c_args_cap} bytes")

        if self.size % 64 != 0:
            r.warn(comp, f"slot size {self.size} not 64-byte aligned")

        return r


# ============================================================
# SYNC GROUP
# ============================================================
class SyncGroup:
    """Synchronization primitives.  Created once in allocation,
    then shared by SharedTaskQueue, WorkerContext, and Supervisor.
    """
    __slots__ = ("lock", "batch_lock", "log_queue")

    def __init__(self,
                 lock: Lock = None,
                 batch_lock: Lock = None,
                 log_queue: Queue = None):
        self.lock = lock or Lock()
        self.batch_lock = batch_lock or Lock()
        self.log_queue = log_queue or Queue()


# ============================================================
# SHARED REFS
# ============================================================
class SharedRefs:
    """Shared-memory names for cross-process attachment."""
    __slots__ = ("slots_name", "state_name", "status_name")

    def __init__(self, slots_name: str, state_name: str, status_name: str):
        self.slots_name = slots_name
        self.state_name = state_name
        self.status_name = status_name


# ============================================================
# WORKER CONFIG
# ============================================================
class WorkerConfig:
    """Behavioral configuration for worker processes."""
    __slots__ = ("batch_size", "admin_frequency",
                 "debug_task_delay", "handler_module",
                 "supervisor_pid")

    def __init__(self,
                 batch_size: int = 64,
                 admin_frequency: int = 5,
                 debug_task_delay: float = 0.0,
                 handler_module: str = None,
                 supervisor_pid: int = 0):
        self.batch_size = batch_size
        self.admin_frequency = admin_frequency
        self.debug_task_delay = debug_task_delay
        self.handler_module = handler_module
        self.supervisor_pid = supervisor_pid

    def validate(self) -> ValidationResult:
        """Validate worker configuration."""
        r = ValidationResult()
        comp = "Worker"

        if self.batch_size > 0:
            r.ok(comp, f"batch_size: {self.batch_size}")
        else:
            r.fail(comp, f"batch_size must be > 0, got {self.batch_size}")

        if self.admin_frequency > 0:
            r.ok(comp, f"admin_frequency: {self.admin_frequency}")
        else:
            r.fail(comp, f"admin_frequency must be > 0, got {self.admin_frequency}")

        if self.debug_task_delay >= 0:
            if self.debug_task_delay > 0:
                r.warn(comp, f"debug_task_delay: {self.debug_task_delay}s (slows workers)")
            else:
                r.ok(comp, "debug_task_delay: 0 (no artificial delay)")
        else:
            r.fail(comp, f"debug_task_delay must be >= 0, got {self.debug_task_delay}")

        if self.handler_module:
            r.ok(comp, f"handler_module: '{self.handler_module}' (loaded at spawn)")

        return r


# ============================================================
# SUPERVISOR CONFIG
# ============================================================
class SupervisorConfig:
    """Behavioral configuration for the supervisor loop."""
    __slots__ = ("display", "auto_terminate",
                 "poll_interval", "idle_check_interval")

    def __init__(self,
                 display: bool = True,
                 auto_terminate: bool = True,
                 poll_interval: float = 0.05,
                 idle_check_interval: int = 10):
        self.display = display
        self.auto_terminate = auto_terminate
        self.poll_interval = poll_interval
        self.idle_check_interval = idle_check_interval

    def validate(self) -> ValidationResult:
        """Validate supervisor configuration."""
        r = ValidationResult()
        comp = "Supervisor"

        if self.poll_interval > 0:
            r.ok(comp, f"poll_interval: {self.poll_interval}s")
        else:
            r.fail(comp, f"poll_interval must be > 0, got {self.poll_interval}")

        if self.idle_check_interval > 0:
            r.ok(comp, f"idle_check_interval: {self.idle_check_interval}")
        else:
            r.fail(comp, f"idle_check_interval must be > 0, got {self.idle_check_interval}")

        if not self.auto_terminate:
            r.warn(comp, "auto_terminate disabled — workers won't stop on idle")

        return r


# ============================================================
# MPOP CONFIG — CENTRAL STORE + FACTORIES + VALIDATION
# ============================================================
class MpopConfig:
    """Central configuration for the entire mpop system.

    Absorbs ALL user-facing parameters from MpopApi.__init__().
    Sub-configs are derived once; factory methods produce
    downstream objects so allocation is just direct assignments.

    Validation:
        result = cfg.validate()          # full check, returns ValidationResult
        result = cfg.validate(registry)  # also checks registered functions
        cfg.print_validation()           # validate + print report
    """
    __slots__ = (
        "num_workers", "queue_slots", "queue_name",
        "validate_enabled", "delimiter",
        "slot", "worker", "supervisor",
    )

    def __init__(self,
                 workers: int = 4,
                 queue_slots: int = 4096,
                 slot_class: Type[ctypes.Structure] = None,

                 display: bool = True,
                 auto_terminate: bool = True,

                 debug: bool = False,
                 debug_delay: float = 0.0,

                 delimiter: str = ' ',
                 handler_module: str = None,
                 worker_batch_size: int = 16,

                 poll_interval: float = 0.05,
                 idle_check_interval: int = 10,
                 queue_name: str = "mpop",
                 validate: bool = True):

        if slot_class is None:
            from .slot import TaskSlot
            slot_class = TaskSlot

        debug_delay = debug_delay if not debug else (debug_delay or 0.1)

        self.num_workers = workers
        self.queue_slots = queue_slots
        self.queue_name = queue_name
        self.validate_enabled = validate
        self.delimiter = delimiter

        self.slot = SlotConfig(slot_class)

        self.worker = WorkerConfig(
            batch_size=worker_batch_size,
            admin_frequency=idle_check_interval,
            debug_task_delay=debug_delay,
            handler_module=handler_module,
        )

        self.supervisor = SupervisorConfig(
            display=display,
            auto_terminate=auto_terminate,
            poll_interval=poll_interval,
            idle_check_interval=idle_check_interval,
        )

    # ---- Validation ----

    def validate(self, registry=None) -> ValidationResult:
        """Full validation of all configuration.

        Checks queue params, then delegates to each sub-config.
        Optionally validates a FunctionRegistry if provided.
        Returns ValidationResult (never raises).
        """
        r = ValidationResult()
        comp = "Queue"

        # Queue-level checks
        if self.num_workers > 0 and self.num_workers <= 64:
            r.ok(comp, f"num_workers: {self.num_workers}")
        elif self.num_workers <= 0:
            r.fail(comp, f"num_workers must be > 0, got {self.num_workers}")
        else:
            r.fail(comp, f"num_workers must be <= 64, got {self.num_workers}")

        if self.queue_slots > 0 and (self.queue_slots & (self.queue_slots - 1)) == 0:
            r.ok(comp, f"queue_slots: {self.queue_slots} (power of 2)")
        elif self.queue_slots <= 0:
            r.fail(comp, f"queue_slots must be > 0, got {self.queue_slots}")
        else:
            r.fail(comp, f"queue_slots must be power of 2, got {self.queue_slots}")

        if self.queue_slots > 0 and self.num_workers > 0:
            if self.queue_slots < self.num_workers:
                r.warn(comp, f"queue_slots ({self.queue_slots}) < "
                       f"num_workers ({self.num_workers}) — workers may starve")

        # Cross-config checks
        if (self.worker.batch_size > 0 and self.queue_slots > 0
                and self.worker.batch_size > self.queue_slots):
            r.warn("Worker", f"batch_size ({self.worker.batch_size}) > "
                   f"queue_slots ({self.queue_slots}) — capped to available")

        # Delegate to sub-configs
        r.merge(self.slot.validate())
        r.merge(self.worker.validate())
        r.merge(self.supervisor.validate())

        # Optional registry validation
        if registry is not None:
            r.merge(registry.validate(self.slot))

        return r

    def check(self):
        """Quick validation — raises ValueError on first failure.

        Used internally by allocate() as a fast gate.
        For detailed diagnostics, use validate() + print_validation().
        """
        result = self.validate()
        if not result.passed:
            errors = [n for n in result.notes if n.level == _FAIL]
            msg = "; ".join(repr(e) for e in errors)
            raise ValueError(msg)

    def print_validation(self, registry=None):
        """Validate and print full report."""
        result = self.validate(registry)
        result.print_report()
        return result

    # ---- Factory methods ----

    def create_sync(self) -> SyncGroup:
        """Create fresh sync primitives."""
        return SyncGroup()

    def create_queue(self, sync: SyncGroup):
        """Create SharedTaskQueue from stored params."""
        from .queues import SharedTaskQueue
        return SharedTaskQueue(
            queue_id=1,
            num_slots=self.queue_slots,
            slot_cfg=self.slot,
            sync=sync,
            queue_name=self.queue_name,
        )

    def create_registry(self):
        """Create FunctionRegistry from stored params."""
        from .registry import FunctionRegistry
        return FunctionRegistry(
            slot_cfg=self.slot,
            delimiter=self.delimiter,
        )