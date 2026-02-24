# ============================================================
# API/MPOP.PY — User-facing API (typed handlers only)
# ============================================================

import inspect
from typing import Optional, Dict, Any, Callable, List
from multiprocessing import Process

from .slot import TaskSlot, ProcTaskFnID, MAX_ARGS
from .queues import SharedTaskQueue
from .allocation import allocate, AllocationResult
from .supervisor import SupervisorController
from .registry import FunctionRegistry
from .worker import worker_process_entry
from .types import HandlerMeta
from .errors import RegistryError, TypeResolutionError


# ============================================================
# MPOP API
# ============================================================
class MpopApi:

    def __init__(self,
                 workers: int = 4,
                 queue_slots: int = 4096,

                 display: bool = True,
                 auto_terminate: bool = True,

                 debug: bool = False,
                 debug_delay: float = 0.0,

                 handler_module: str = None,
                 worker_batch_size: int = 16,

                 poll_interval: float = 0.05,
                 idle_check_interval: int = 10,
                 queue_name: str = "mpop",
                 ):
        self._workers = workers
        self._queue_slots = queue_slots
        self._display = display
        self._auto_terminate = auto_terminate
        self._debug = debug
        self._debug_delay = debug_delay if not debug else (debug_delay or 0.1)
        self._poll_interval = poll_interval
        self._idle_check_interval = idle_check_interval
        self._queue_name = queue_name
        self._handler_module = handler_module
        self._worker_batch_size = worker_batch_size

        self._registry = FunctionRegistry()

        self._result: Optional[AllocationResult] = None
        self._supervisor: Optional[SupervisorController] = None

        self._allocate()

    def _allocate(self):
        """Allocate shared memory, create worker contexts."""
        self._result = allocate(
            num_workers=self._workers,
            queue_slots=self._queue_slots,
            queue_name=self._queue_name,
            debug_task_delay=self._debug_delay,
            admin_frequency=self._idle_check_interval,
            handler_module=self._handler_module,
            worker_batch_size=self._worker_batch_size,
        )

    # ========================================================
    # PROPERTIES
    # ========================================================
    @property
    def queue(self) -> SharedTaskQueue:
        return self._result.queue

    @property
    def num_workers(self) -> int:
        return self._workers

    @property
    def registry(self) -> FunctionRegistry:
        return self._registry

    # ========================================================
    # DECORATOR — @app.task()
    # ========================================================
    def task(self, fn_id: int = None, name: str = None):
        """Register a typed handler via decorator.

        All parameters MUST have type annotations.
        Type resolution happens once at decoration time.

        Usage:
            @app.task(fn_id=0x8000)
            def multiply(a: int, b: float) -> float:
                return a * b
        """
        def decorator(handler: Callable):
            assigned_id = self._registry.register(
                handler=handler, fn_id=fn_id, name=name or handler.__name__,
            )
            handler._mpop_fn_id = assigned_id
            return handler
        return decorator

    # ========================================================
    # REGISTER FROM MODULE — load HANDLERS dict
    # ========================================================
    def register_handlers(self, module=None, handlers_dict: Dict[int, Callable] = None):
        """Register handlers from a module's HANDLERS dict or explicit dict.
        All handlers must have full type annotations.
        """
        if module is not None:
            if not hasattr(module, 'HANDLERS'):
                raise RegistryError(f"Module {module.__name__} has no HANDLERS dict")
            handlers_dict = module.HANDLERS

        if handlers_dict is None:
            raise RegistryError("No handlers provided")

        for fid, handler in handlers_dict.items():
            self._registry.register(handler=handler, fn_id=fid)
            handler._mpop_fn_id = fid

    # ========================================================
    # VALIDATE
    # ========================================================
    def validate(self):
        """Print summary of all registered handlers."""
        self._registry.validate()

    # ========================================================
    # ENQUEUE — zero validation, two styles
    # ========================================================
    def enqueue(self, fn=None, *, fn_id: int = None,
                args: tuple = (), tsk_id: int = 0,
                blocking: bool = False, timeout: float = 10.0,
                **kwargs) -> bool:
        """Enqueue a task.

        Pythonic:   app.enqueue(multiply, a=10, b=3.14)
        Positional: app.enqueue(fn_id=0x8000, args=(10, 3.14))

        Zero validation at enqueue time.
        """
        slot = TaskSlot()
        slot.tsk_id = tsk_id

        if fn is not None:
            fid = getattr(fn, '_mpop_fn_id', None)
            if fid is None:
                raise ValueError(f"Function {fn.__name__} not registered")

            slot.fn_id = fid
            meta = self._registry.get(fid)
            if meta is None:
                raise ValueError(f"fn_id {fid:#06x} not in registry")

            if kwargs:
                packed = meta.pack_fn(**kwargs)
            elif args:
                packed = meta.pack_positional_fn(args)
            else:
                packed = ()
            for i, v in enumerate(packed):
                slot.args[i] = v

        elif fn_id is not None:
            slot.fn_id = fn_id
            meta = self._registry.get(fn_id)

            if meta is not None:
                packed = meta.pack_positional_fn(args)
                for i, v in enumerate(packed):
                    slot.args[i] = v
            else:
                # System fn_ids (TERMINATE, INCREMENT, etc.) — raw int writes
                for i, v in enumerate(args):
                    if i < MAX_ARGS:
                        slot.args[i] = int(v)
        else:
            slot.fn_id = ProcTaskFnID.INCREMENT
            for i, v in enumerate(args):
                if i < MAX_ARGS:
                    slot.args[i] = int(v)

        if blocking:
            return self._result.queue.enqueue_blocking(slot, timeout)
        return self._result.queue.enqueue(slot)

    # ========================================================
    # ENQUEUE MANY
    # ========================================================
    def enqueue_many(self, tasks, blocking: bool = False) -> int:
        """Enqueue multiple tasks. Returns count of successfully enqueued.

        Accepts list of:
        - Tuples: (fn, kwargs_dict) or (fn, arg1, arg2, ...)
        - Dicts:  {'fn': handler, 'a': 10, 'b': 3.14}
                  {'fn_id': 0x8000, 'args': (10, 3.14)}
        """
        count = 0
        for t in tasks:
            ok = False
            if isinstance(t, tuple):
                fn = t[0]
                if len(t) == 2 and isinstance(t[1], dict):
                    ok = self.enqueue(fn, blocking=blocking, **t[1])
                else:
                    ok = self.enqueue(fn, args=t[1:], blocking=blocking)
            elif isinstance(t, dict):
                fn = t.pop('fn', None)
                fn_id_val = t.pop('fn_id', None)
                args_val = t.pop('args', ())
                tsk_id_val = t.pop('tsk_id', 0)
                remaining = t
                if fn is not None:
                    ok = self.enqueue(fn, args=args_val, tsk_id=tsk_id_val,
                                      blocking=blocking, **remaining)
                elif fn_id_val is not None:
                    ok = self.enqueue(fn_id=fn_id_val, args=args_val,
                                      tsk_id=tsk_id_val, blocking=blocking)
            if ok:
                count += 1
        return count

    # ========================================================
    # RUN
    # ========================================================
    def run(self, enqueue_callback: Callable = None) -> int:
        """Start supervisor loop. Blocks until all tasks complete.

        Builds Process objects with injected handlers_map and pool_proxy,
        creates supervisor, then runs.
        """
        # Build handlers_map from registry for direct passing to workers
        handlers_map = {
            meta.fn_id: meta.handler
            for meta in self._registry._handlers.values()
        }

        # Pool proxy (lazy — may be None if no REFERENCE types used)
        proxy = self._registry.pool.pool_proxy

        # Inject into contexts and build fresh Process objects
        processes = []
        for wctx in self._result.worker_contexts:
            wctx.handlers_map = handlers_map if handlers_map else None
            wctx.pool_proxy = proxy
            p = Process(target=worker_process_entry, args=(wctx,))
            processes.append(p)

        # Build supervisor with final processes
        supervisor = SupervisorController(
            shared_queue=self._result.queue,
            status_shm=self._result.status_shm,
            processes=processes,
            log_queue=self._result.log_queue,
            num_workers=self._result.num_workers,
            display=self._display,
            auto_terminate=self._auto_terminate,
            poll_interval=self._poll_interval,
            idle_check_interval=self._idle_check_interval,
        )

        return supervisor.run(enqueue_callback=enqueue_callback)

    # ========================================================
    # STATUS
    # ========================================================
    def status(self) -> Dict:
        return {
            'queue_occupancy': self.queue.get_actual_occupancy(),
            'queue_capacity': self._queue_slots,
            'workers': self._workers,
            'display': self._display,
            'registered_functions': len(self._registry),
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
        print("=" * 50)

    def list_functions(self) -> List[Dict]:
        return self._registry.list_functions()

    @classmethod
    def simple(cls, workers=2, display=False) -> 'MpopApi':
        return cls(workers=workers, queue_slots=256, display=display)

    @classmethod
    def debug(cls, workers=2, delay=0.1) -> 'MpopApi':
        return cls(workers=workers, debug=True, debug_delay=delay)


Mpop = MpopApi


def _cleanup_result(result):
    """Cleanup AllocationResult resources. Used by MpopApi.__del__."""
    if result is None:
        return
    try:
        result.queue.release_views()
    except Exception:
        pass
    import gc
    gc.collect()
    try:
        result.queue.close()
        result.queue.unlink()
    except Exception:
        pass
    try:
        result.status_shm.close()
        result.status_shm.unlink()
    except Exception:
        pass