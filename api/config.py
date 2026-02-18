import ctypes
from multiprocessing import Lock, Queue
from typing import Type


###Slot
class SlotConfig:
    __slots__ = ("cls", "cls_name", "size", "int_args_cap", "c_args_cap")
    def __init__(self, slot_class: Type[ctypes.Structure]):
        from .slot import has_char_args
        self.cls = slot_class
        self.cls_name = slot_class.__name__
        self.size = ctypes.sizeof(slot_class)
        self.int_args_cap =0
        self.c_args_cap =0
        for fname, ftype, *_ in slot_class._fields_:
            if fname == 'args':
                self.int_args_cap = ftype._length_
            elif fname =='c_args' and has_char_args(slot_class):
                self.c_args_cap = ftype._length_

    @property
    def has_c_args(self) -> bool:
        return self.c_args_cap > 0
    def make_array(self, count: int):
        return self.cls * count

    def build_slot(self, tsk_id: int = 0, fn_id: int = 0,
                   args: tuple = (), c_args: bytes = b'',
                   pool_id: int = 0):
        slot = self.cls()
        slot.tsk_id = tsk_id
        slot.fn_id = fn_id

        cap =self.int_args_cap
        for i in range(len(args)):
            if i >= cap:
                break
            slot.args[i] = args[i]

        if pool_id > 0:
            slot.meta[0] = pool_id & 0xFF
            slot.meta[1] = (pool_id >> 8) & 0xFF
            slot.meta[2] = (pool_id >> 16) & 0xFF
            slot.meta[3] = (pool_id >> 24) & 0xFF

        if c_args and self.c_args_cap > 0:
            slot.c_args = c_args[:self.c_args_cap]

        return slot


class SyncGroup:
    __slots__ = ("lock", "batch_lock", "log_queue")

    def __init__(self,
                 lock: Lock = None,
                 batch_lock: Lock = None,
                 log_queue: Queue = None):
        self.lock = lock or Lock()
        self.batch_lock = batch_lock or Lock()
        self.log_queue = log_queue or Queue()


class SharedRefs:
    __slots__ = ("slots_name", "state_name", "status_name")

    def __init__(self, slots_name: str, state_name: str, status_name: str):
        self.slots_name = slots_name
        self.state_name = state_name
        self.status_name = status_name


class WorkerConfig:
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

class SupervisorConfig:
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


class MpopConfig:
    __slots__ = (
        "num_workers", "queue_slots", "queue_name",
        "validate", "delimiter",
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
            from .slot import TaskSlot128_cargs
            slot_class = TaskSlot128_cargs

        debug_delay = debug_delay if not debug else (debug_delay or 0.1)

        # ---- Store core params ----
        self.num_workers = workers
        self.queue_slots = queue_slots
        self.queue_name = queue_name
        self.validate = validate
        self.delimiter = delimiter

        # ---- Derive sub-configs (computed once) ----
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

    def check(self):
        from .errors import (ErrorCode, Component, format_error,
                             validate_num_slots, validate_num_workers)
        err = validate_num_workers(self.num_workers)
        if err:
            raise ValueError(format_error(
                ErrorCode.E003_NUM_WORKERS_EXCEEDED, Component.ALLOCATION, err))
        err = validate_num_slots(self.queue_slots)
        if err:
            raise ValueError(format_error(
                ErrorCode.E002_INVALID_NUM_SLOTS, Component.ALLOCATION, err))

    def create_sync(self) -> SyncGroup:
        return SyncGroup()

    def create_queue(self, sync: SyncGroup):
        from .queues import SharedTaskQueue
        return SharedTaskQueue(
            queue_id=1,
            num_slots=self.queue_slots,
            slot_cfg=self.slot,
            sync=sync,
            queue_name=self.queue_name,
        )

    def create_registry(self):
        from .registry import FunctionRegistry
        return FunctionRegistry(
            slot_cfg=self.slot,
            delimiter=self.delimiter,
        )