# ============================================================
# API/MPOP.PY
# ============================================================
# MpopApi: Primary user-facing entry point.
# 
# Central flow:
#   __init__() -> all config
#   run()      -> execute (single user-facing method)
# ============================================================

import ctypes
from typing import Type, Optional, Dict, Any, Callable, Union, List

from .slot import (TaskSlot128, TaskSlot128_cargs, ProcTaskFnID, has_char_args)
from .queues import SharedTaskQueue
from .allocation import allocate, AllocationResult
from .supervisor import SupervisorController
from .registry import FunctionRegistry, ArgValidationError, RegistryError


#============================================================
# VALIDATION ERROR (re-export for compatibility)
#============================================================
class ValidationError(ArgValidationError):
    pass


#============================================================
# MPOP API
#============================================================
class MpopApi:
    '''
    Primary user-facing API.
    
    Central flow:
        1. __init__(): All configuration (display, workers, etc.)
        2. enqueue(): Add tasks (before run)
        3. run(): Execute until completion
    
    Function Registration:
        app.register(handler, name="my_func", arg_count=2)
        app.set_var("config", {"key": "value"})
        app.enqueue(fn_id=fn_id, args=(1,2), c_args="var:config")
    
    Usage:
        # Simple
        app = MpopApi(workers=4)
        for i in range(100):
            app.enqueue(args=(i, 1))
        app.run()
        
        # With custom function
        app = MpopApi(workers=4)
        fn_id = app.register(my_handler, name="compute")
        app.enqueue(fn_id=fn_id, args=(10,))
        app.run()
        
        # With variable reference
        app.set_var("data", [1,2,3,4,5])
        app.enqueue(fn_id=fn_id, c_args="var:data")
    '''
    
    DEFAULT_SLOT = TaskSlot128_cargs
    
    def __init__(self,
                 workers: int = 4,
                 queue_slots: int = 4096,
                 slot_class: Type[ctypes.Structure] = None,
                 
                 # Display: True (default) shows TTY, False suppresses
                 display: bool = True,
                 
                 # Auto terminate when idle
                 auto_terminate: bool = True,
                 
                 # Debug
                 debug: bool = False,
                 debug_delay: float = 0.0,
                 
                 # Parser delimiter for c_args
                 delimiter: str = ' ',
                 
                 # Advanced
                 poll_interval: float = 0.05,
                 idle_check_interval: int = 10,
                 queue_name: str = "mpop",
                 validate: bool = True,
                 ):
        '''
        Configure the task queue.
        
        Args:
            workers: Number of worker processes
            queue_slots: Queue capacity (power of 2)
            slot_class: Task slot type (default: TaskSlot128_cargs)
            
            display: Show TTY display (default: True)
            
            auto_terminate: Terminate when queue empty and idle (default: True)
            
            debug: Enable debug mode
            debug_delay: Delay per task (seconds)
            
            delimiter: c_args parsing delimiter (default: space)
            
            poll_interval: Display update interval
            idle_check_interval: Check terminate every N polls
            queue_name: Queue identifier
            validate: Enable arg validation
        '''
        self._workers = workers
        self._queue_slots = queue_slots
        self._slot_class = slot_class or self.DEFAULT_SLOT
        self._display = display
        self._auto_terminate = auto_terminate
        self._debug = debug
        self._debug_delay = debug_delay if not debug else (debug_delay or 0.1)
        self._poll_interval = poll_interval
        self._idle_check_interval = idle_check_interval
        self._queue_name = queue_name
        self._validate = validate
        
        # Get slot capacities
        slot_instance = self._slot_class()
        self._slot_int_args = len(slot_instance.args)
        self._slot_c_args = len(slot_instance.c_args) if has_char_args(self._slot_class) else 0
        
        # Function registry with args pool
        self._registry = FunctionRegistry(
            slot_int_args=self._slot_int_args,
            slot_c_args=self._slot_c_args,
            delimiter=delimiter,
        )
        
        self._result: Optional[AllocationResult] = None
        self._supervisor: Optional[SupervisorController] = None
        
        self._allocate()
    
    def _allocate(self):
        '''Allocate resources.'''
        self._result = allocate(
            num_workers=self._workers,
            queue_slots=self._queue_slots,
            slot_class=self._slot_class,
            queue_name=self._queue_name,
            debug_task_delay=self._debug_delay,
            admin_frequency=self._idle_check_interval,
        )
        
        self._supervisor = SupervisorController(
            shared_queue=self._result.queue,
            status_shm=self._result.status_shm,
            processes=self._result.processes,
            log_queue=self._result.log_queue,
            num_workers=self._result.num_workers,
            display=self._display,
            auto_terminate=self._auto_terminate,
            poll_interval=self._poll_interval,
            idle_check_interval=self._idle_check_interval,
        )
    
    #==========================================================
    # PROPERTIES
    #==========================================================
    @property
    def queue(self) -> SharedTaskQueue:
        return self._result.queue
    
    @property
    def slot_class(self) -> Type:
        return self._slot_class
    
    @property
    def num_workers(self) -> int:
        return self._workers
    
    @property
    def registry(self) -> FunctionRegistry:
        '''Access to function registry.'''
        return self._registry
    
    @property
    def delimiter(self) -> str:
        '''c_args parsing delimiter.'''
        return self._registry.delimiter
    
    @delimiter.setter
    def delimiter(self, value: str):
        '''Set c_args parsing delimiter.'''
        self._registry.delimiter = value
    
    #==========================================================
    # FUNCTION REGISTRATION
    #==========================================================
    def register(self,
                 handler: Callable,
                 name: str = None,
                 fn_id: int = None,
                 arg_count: int = 0,
                 meta: Dict = None) -> int:
        '''
        Register a function handler.
        
        Args:
            handler: Function(slot, ctx) -> Any
            name: Function name (default: handler.__name__)
            fn_id: Explicit fn_id (default: auto-assign from 0x8000)
            arg_count: Expected int args count (0 = any)
            meta: Additional metadata
            
        Returns:
            fn_id
            
        Example:
            fn_id = app.register(my_handler, name="compute", arg_count=2)
            app.enqueue(fn_id=fn_id, args=(10, 20))
        '''
        return self._registry.register(
            handler=handler,
            name=name,
            fn_id=fn_id,
            arg_count=arg_count,
            meta=meta,
        )
    
    def function(self, name: str = None, arg_count: int = 0):
        '''
        Decorator to register a function.
        
        Example:
            @app.function(name="compute", arg_count=2)
            def my_handler(slot, ctx):
                return slot.args[0] + slot.args[1]
            
            app.enqueue(fn_id=my_handler.fn_id, args=(10, 20))
        '''
        def decorator(handler):
            fn_id = self.register(handler, name=name or handler.__name__, arg_count=arg_count)
            handler.fn_id = fn_id
            return handler
        return decorator
    
    #==========================================================
    # VARIABLES
    #==========================================================
    def set_var(self, name: str, value: Any):
        '''
        Set a named variable for var:name references.
        
        Example:
            app.set_var("config", {"key": "value"})
            app.enqueue(c_args="var:config")
        '''
        self._registry.set_var(name, value)
    
    def get_var(self, name: str) -> Any:
        '''Get a named variable.'''
        return self._registry.get_var(name)
    
    #==========================================================
    # ENQUEUE
    #==========================================================
    def enqueue(self,
                fn_id: int = None,
                args: tuple = (),
                c_args: Union[bytes, str, List[str]] = None,
                tsk_id: int = 0,
                blocking: bool = False,
                timeout: float = 10.0) -> bool:
        '''
        Enqueue a task.
        
        Args:
            fn_id: Function ID (default: INCREMENT)
            args: Integer arguments tuple
            c_args: Char args (bytes, str, list, or "var:name")
            tsk_id: Task ID
            blocking: Use blocking enqueue
            timeout: Timeout for blocking
            
        Returns:
            True if enqueued
            
        Special c_args:
            - "var:name": Reference to variable set with set_var()
            - Oversized c_args: Automatically stored in pool
        '''
        if fn_id is None:
            fn_id = ProcTaskFnID.INCREMENT
        
        # Prepare args (validates and handles pool/var refs)
        if self._validate:
            try:
                args, packed_c_args, pool_id = self._registry.prepare_args(
                    fn_id, args, c_args
                )
            except ArgValidationError as e:
                raise ValidationError(str(e))
        else:
            # No validation, just pack
            packed_c_args = b''
            pool_id = 0
            if c_args is not None:
                if isinstance(c_args, str):
                    packed_c_args = c_args.encode('utf-8') + b'\x00'
                elif isinstance(c_args, list):
                    packed_c_args = self._registry.delimiter.encode('utf-8').join(
                        s.encode('utf-8') for s in c_args
                    ) + b'\x00'
                else:
                    packed_c_args = c_args
        
        # Create slot
        task = self._slot_class()
        task.tsk_id = tsk_id
        task.fn_id = fn_id
        
        # Set int args
        for i, v in enumerate(args):
            if i < len(task.args):
                task.args[i] = v
        
        # Set pool_id in meta if used
        if pool_id > 0:
            task.meta[0] = pool_id & 0xFF
            task.meta[1] = (pool_id >> 8) & 0xFF
            task.meta[2] = (pool_id >> 16) & 0xFF
            task.meta[3] = (pool_id >> 24) & 0xFF
        
        # Set c_args
        if packed_c_args and has_char_args(self._slot_class):
            task.c_args = packed_c_args[:len(task.c_args)]
        
        # Enqueue
        if blocking:
            return self._result.queue.enqueue_blocking(task, timeout)
        return self._result.queue.enqueue(task)
    
    def enqueue_many(self, tasks: List[Dict], blocking: bool = False) -> int:
        '''Enqueue multiple tasks. Returns count.'''
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
    
    #==========================================================
    # RUN
    #==========================================================
    def run(self, enqueue_callback: Callable = None) -> int:
        '''
        Run until completion.
        
        This is the single user-facing execution method.
        Display is shown if enabled in __init__ (default: True).
        
        Args:
            enqueue_callback: Optional func() for dynamic enqueue.
                             Called each loop iteration.
                             Return False or raise StopIteration to stop.
        
        Returns:
            Exit code (0=success, 1=interrupted)
        '''
        return self._supervisor.run(enqueue_callback=enqueue_callback)
    
    #==========================================================
    # STATUS
    #==========================================================
    def status(self) -> Dict:
        '''Get current status.'''
        return {
            'queue_occupancy': self.queue.get_actual_occupancy(),
            'queue_capacity': self._queue_slots,
            'workers': self._workers,
            'display': self._display,
            'registered_functions': len(self._registry),
        }
    
    def print_status(self):
        '''Print status.'''
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
        '''List registered functions.'''
        return self._registry.list_functions()
    
    #==========================================================
    # FACTORY
    #==========================================================
    @classmethod
    def simple(cls, workers: int = 2, display: bool = False) -> 'MpopApi':
        '''Create minimal instance.'''
        return cls(workers=workers, queue_slots=256, display=display)
    
    @classmethod
    def debug(cls, workers: int = 2, delay: float = 0.1) -> 'MpopApi':
        '''Create debug instance.'''
        return cls(workers=workers, debug=True, debug_delay=delay)


# Alias
Mpop = MpopApi