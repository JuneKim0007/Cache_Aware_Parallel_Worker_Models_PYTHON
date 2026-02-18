# ============================================================
# API/MPOP.PY
# ============================================================
import ctypes
from typing import Type, Optional, Dict, Any, Callable, Union, List

from .slot import ProcTaskFnID
from .config import MpopConfig
from .queues import SharedTaskQueue
from .allocation import allocate, AllocationResult
from .supervisor import SupervisorController
from .registry import FunctionRegistry
from .args import pack_c_args
from .errors import ArgValidationError, RegistryError


class ValidationError(ArgValidationError):
    __slots__ = ()


class MpopApi:
    """User-facing API.

    Public interface is unchanged.  Internally, a single MpopConfig
    absorbs all constructor params.  allocate(cfg) and
    result.create_supervisor() use cfg directly — no param unpacking.
    """

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
                 validate: bool = True,
                 ):
        # ---- Single config from user params ----
        self._cfg = MpopConfig(
            workers=workers,
            queue_slots=queue_slots,
            slot_class=slot_class,
            display=display,
            auto_terminate=auto_terminate,
            debug=debug,
            debug_delay=debug_delay,
            delimiter=delimiter,
            handler_module=handler_module,
            worker_batch_size=worker_batch_size,
            poll_interval=poll_interval,
            idle_check_interval=idle_check_interval,
            queue_name=queue_name,
            validate=validate,
        )

        # Registry — created from cfg
        self._registry = self._cfg.create_registry()

        self._result: Optional[AllocationResult] = None
        self._supervisor: Optional[SupervisorController] = None

        self._allocate()

    def _allocate(self):
        """Allocate shared resources and build supervisor."""
        self._result = allocate(self._cfg)
        self._supervisor = self._result.create_supervisor()

    @property
    def queue(self) -> SharedTaskQueue:
        return self._result.queue

    @property
    def slot_class(self) -> Type:
        return self._cfg.slot.cls

    @property
    def num_workers(self) -> int:
        return self._cfg.num_workers

    @property
    def registry(self) -> FunctionRegistry:
        return self._registry

    @property
    def delimiter(self) -> str:
        return self._registry.delimiter

    @delimiter.setter
    def delimiter(self, value: str):
        self._registry.delimiter = value

    def register(self,
                 handler: Callable,
                 name: str = None,
                 fn_id: int = None,
                 arg_count: int = 0,
                 meta: Dict = None) -> int:
        return self._registry.register(
            handler=handler,
            name=name,
            fn_id=fn_id,
            arg_count=arg_count,
            meta=meta,
        )

    def function(self, name: str = None, arg_count: int = 0):
        def decorator(handler):
            fn_id = self.register(handler, name=name or handler.__name__, arg_count=arg_count)
            handler.fn_id = fn_id
            return handler
        return decorator

    def set_var(self, name: str, value: Any):
        self._registry.set_var(name, value)

    def get_var(self, name: str) -> Any:
        return self._registry.get_var(name)

    def share(self, name: str, value: Any):
        self._registry.set_shared(name, value)

    def get_shared(self, name: str) -> Any:
        return self._registry.get_shared(name)

    def has_shared(self, name: str) -> bool:
        return self._registry.has_shared(name)

    def list_shared(self) -> List[str]:
        return self._registry.list_shared()

    # ==========================================================
    # ENQUEUE
    # ==========================================================
    def enqueue(self,
                fn_id: int = None,
                args: tuple = (),
                c_args: Union[bytes, str, List[str]] = None,
                tsk_id: int = 0,
                blocking: bool = False,
                timeout: float = 10.0) -> bool:
        if fn_id is None:
            fn_id = ProcTaskFnID.INCREMENT

        cfg = self._cfg

        # Prepare args (validates and handles pool/var refs)
        if cfg.validate:
            try:
                args, packed_c_args, pool_id = self._registry.prepare_args(
                    fn_id, args, c_args
                )
            except ArgValidationError as e:
                raise ValidationError(str(e))
        else:
            packed_c_args = pack_c_args(c_args, self._registry.delimiter) if c_args is not None else b''
            pool_id = 0

        # Build slot
        task = cfg.slot.build_slot(
            tsk_id=tsk_id, fn_id=fn_id, args=args,
            c_args=packed_c_args, pool_id=pool_id,
        )

        # Enqueue
        if blocking:
            return self._result.queue.enqueue_blocking(task, timeout)
        return self._result.queue.enqueue(task)

    def enqueue_many(self, tasks: List[Dict], blocking: bool = False) -> int:
        count = 0
        for t in tasks:
            if self.enqueue(
                fn_id=t.get('fn_id'),
                args=t.get('args', ()),
                c_args=t.get('c_args'),
                tsk_id=t.get('tsk_id', 0),
                blocking=blocking,
            ):
                count += 1
        return count

    def run(self, enqueue_callback: Callable = None) -> int:
        return self._supervisor.run(enqueue_callback=enqueue_callback)

    def status(self) -> Dict:
        cfg = self._cfg
        return {
            'queue_occupancy': self.queue.get_actual_occupancy(),
            'queue_capacity': cfg.queue_slots,
            'workers': cfg.num_workers,
            'display': cfg.supervisor.display,
            'registered_functions': len(self._registry),
            'shared_variables': len(self._registry.list_shared()),
        }

    def print_status(self):
        s = self.status()
        print("=" * 50)
        print("MpopApi Status")
        print("=" * 50)
        print(f"Workers: {s['workers']}")
        print(f"Display: {s['display']}")
        print(f"Queue: {s['queue_occupancy']}/{s['queue_capacity']}")
        print(f"Registered functions: {s['registered_functions']}")
        print(f"Shared variables: {s['shared_variables']}")
        print("=" * 50)

    def list_functions(self) -> List[Dict]:
        return self._registry.list_functions()

    @classmethod
    def simple(cls, workers: int = 2, display: bool = False) -> 'MpopApi':
        return cls(workers=workers, queue_slots=256, display=display)

    @classmethod
    def debug(cls, workers: int = 2, delay: float = 0.1) -> 'MpopApi':
        return cls(workers=workers, debug=True, debug_delay=delay)


# Alias
Mpop = MpopApi