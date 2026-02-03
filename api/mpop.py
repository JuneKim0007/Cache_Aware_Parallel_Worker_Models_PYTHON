# ============================================================
# API/MPOP.PY
# ============================================================
# MpopApi: Primary user-facing entry point.
# ============================================================

import ctypes
from typing import Type, Optional, Dict, Any, Callable, Union, List
from dataclasses import dataclass

from .slot import (TaskSlot128, TaskSlot128_cargs, TaskSlot196, TaskSlot196_cargs,
                   ProcTaskFnID, SlotVariant, SLOT_REGISTRY, has_char_args)
from .queues import SharedTaskQueue
from .allocation import allocate, cleanup, AllocationResult
from .supervisor import SupervisorController
from .worker import (WorkerStatusStruct, STATE_NAMES, STATE_INIT,
                     STATE_RUNNING, STATE_IDLE, STATE_TERMINATED)
from .tasks import TaskDispatcher, TaskResult, TaskContext
from .args import ArgParser


#============================================================
# FUNCTION REGISTRY
#============================================================
_USER_FN_ID_START = 0x8000
_user_fn_counter = _USER_FN_ID_START

def _next_fn_id() -> int:
    global _user_fn_counter
    fn_id = _user_fn_counter
    _user_fn_counter += 1
    return fn_id


#============================================================
# VALIDATION
#============================================================
class ValidationError(Exception):
    pass


def _validate_task_args(slot_class: Type, args: tuple, c_args: bytes = None) -> Optional[str]:
    '''
    Validate task arguments fit in slot.
    Returns error message or None if valid.
    '''
    #Check int args count
    try:
        max_args = len(slot_class().args)
    except:
        max_args = 10  #Default
    
    if len(args) > max_args:
        return f"Too many args: {len(args)} > {max_args}"
    
    #Check c_args length for CHAR_ARGS slots
    if c_args and has_char_args(slot_class):
        try:
            max_cargs = len(slot_class().c_args)
        except:
            max_cargs = 64
        if len(c_args) > max_cargs:
            return f"c_args too long: {len(c_args)} > {max_cargs}"
    
    return None


#============================================================
# MPOP API
#============================================================
class MpopApi:
    '''
    Primary user-facing API for multiprocessing task queue.
    
    Usage:
        app = MpopApi(workers=4)
        
        # Define custom function
        @app.function
        def my_handler(slot, ctx):
            return slot.args[0] * 2
        
        # Enqueue tasks
        app.enqueue(fn_id=my_handler.fn_id, args=(10, 20))
        
        # Run
        app.run()
    '''
    
    #Default slot type
    DEFAULT_SLOT_CLASS = TaskSlot128_cargs
    
    def __init__(self,
                 workers: int = 4,
                 queue_slots: int = 4096,
                 slot_class: Type[ctypes.Structure] = None,
                 
                 #Supervisor options
                 display: bool = True,
                 auto_terminate: bool = True,
                 
                 #Debug options
                 debug: bool = False,
                 debug_delay: float = 0.0,
                 
                 #Advanced options
                 admin_frequency: int = 10,
                 queue_name: str = "mpop",
                 validate: bool = True,
                 ):
        '''
        Args:
            workers: Number of worker processes (default: 4)
            queue_slots: Queue capacity, must be power of 2 (default: 4096)
            slot_class: Task slot type (default: TaskSlot128_cargs)
            
            display: Show terminal UI (default: True)
            auto_terminate: Terminate when queue empty (default: True)
            
            debug: Enable debug mode (default: False)
            debug_delay: Delay per task in seconds (default: 0.0)
            
            admin_frequency: Admin check interval (default: 10)
            queue_name: Queue identifier (default: "mpop")
            validate: Enable argument validation (default: True)
        '''
        #Store config
        self._workers = workers
        self._queue_slots = queue_slots
        self._slot_class = slot_class or self.DEFAULT_SLOT_CLASS
        self._display = display
        self._auto_terminate = auto_terminate
        self._debug = debug
        self._debug_delay = debug_delay if not debug else (debug_delay or 0.1)
        self._admin_frequency = admin_frequency
        self._queue_name = queue_name
        self._validate = validate
        
        #Internal state
        self._result: Optional[AllocationResult] = None
        self._supervisor: Optional[SupervisorController] = None
        self._allocated = False
        self._started = False
        
        #User-defined functions
        self._user_functions: Dict[int, Callable] = {}
        self._fn_names: Dict[int, str] = {}
        
        #Arg parser for c_args
        self._arg_parser = ArgParser()
        
        #Auto-allocate
        self._allocate()
    
    def _allocate(self):
        if self._allocated:
            return
        
        self._result = allocate(
            num_workers=self._workers,
            queue_slots=self._queue_slots,
            slot_class=self._slot_class,
            queue_name=self._queue_name,
            debug_task_delay=self._debug_delay,
            admin_frequency=self._admin_frequency,
        )
        
        self._supervisor = SupervisorController(
            shared_queue=self._result.queue,
            status_shm=self._result.status_shm,
            processes=self._result.processes,
            log_queue=self._result.log_queue,
            num_workers=self._result.num_workers,
            use_display=self._display,
            auto_terminate_on_empty=self._auto_terminate,
        )
        
        self._allocated = True
    
    #==========================================================
    # PROPERTIES
    #==========================================================
    @property
    def queue(self) -> SharedTaskQueue:
        '''Direct access to shared queue.'''
        return self._result.queue
    
    @property
    def slot_class(self) -> Type[ctypes.Structure]:
        return self._slot_class
    
    @property
    def num_workers(self) -> int:
        return self._workers
    
    #==========================================================
    # FUNCTION REGISTRATION
    #==========================================================
    def create_function(self, handler: Callable, name: str = None) -> int:
        '''
        Register a user-defined function handler.
        
        Args:
            handler: Function(slot, ctx) -> Any
            name: Optional name for the function
            
        Returns:
            fn_id to use when enqueueing tasks
            
        Example:
            def my_handler(slot, ctx):
                return slot.args[0] + slot.args[1]
            
            fn_id = app.create_function(my_handler, name="add")
            app.enqueue(fn_id=fn_id, args=(10, 20))
        '''
        fn_id = _next_fn_id()
        self._user_functions[fn_id] = handler
        self._fn_names[fn_id] = name or handler.__name__
        
        #Register with task dispatcher (will be used by workers)
        #Note: Workers have their own dispatcher, so this is for reference
        return fn_id
    
    def function(self, name: str = None):
        '''
        Decorator to register a function handler.
        
        Example:
            @app.function
            def my_handler(slot, ctx):
                return slot.args[0] * 2
            
            # my_handler.fn_id is now available
            app.enqueue(fn_id=my_handler.fn_id, args=(10,))
        '''
        def decorator(handler: Callable):
            fn_id = self.create_function(handler, name=name or handler.__name__)
            handler.fn_id = fn_id
            return handler
        return decorator
    
    def get_function_name(self, fn_id: int) -> str:
        '''Get name of registered function.'''
        if fn_id in self._fn_names:
            return self._fn_names[fn_id]
        if fn_id in ProcTaskFnID.__members__.values():
            return ProcTaskFnID(fn_id).name
        return f"FN_{fn_id:04X}"
    
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
        Enqueue a task with validation.
        
        Args:
            fn_id: Function ID (default: INCREMENT)
            args: Integer arguments as tuple
            c_args: Char arguments (bytes, str, or list of str)
            tsk_id: Task ID
            blocking: Use blocking enqueue
            timeout: Timeout for blocking enqueue
            
        Returns:
            True if enqueued successfully
            
        Raises:
            ValidationError if validation enabled and args invalid
        '''
        #Default fn_id
        if fn_id is None:
            fn_id = ProcTaskFnID.INCREMENT
        
        #Pack c_args if needed
        packed_cargs = None
        if c_args is not None:
            if isinstance(c_args, str):
                packed_cargs = c_args.encode('utf-8') + b'\x00'
            elif isinstance(c_args, list):
                packed_cargs = self._arg_parser.pack_str(c_args)
            else:
                packed_cargs = c_args
        
        #Validate
        if self._validate:
            err = _validate_task_args(self._slot_class, args, packed_cargs)
            if err:
                raise ValidationError(f"Enqueue validation failed: {err}")
        
        #Create task
        task = self._slot_class()
        task.tsk_id = tsk_id
        task.fn_id = fn_id
        
        #Set int args
        for i, val in enumerate(args):
            if i < len(task.args):
                task.args[i] = val
        
        #Set c_args if slot supports it
        if packed_cargs and has_char_args(self._slot_class):
            max_len = len(task.c_args)
            task.c_args = packed_cargs[:max_len]
        
        #Enqueue
        if blocking:
            return self._result.queue.enqueue_blocking(task, timeout)
        else:
            return self._result.queue.enqueue(task)
    
    def enqueue_many(self,
                     tasks: List[Dict],
                     blocking: bool = False) -> int:
        '''
        Enqueue multiple tasks.
        
        Args:
            tasks: List of dicts with keys: fn_id, args, c_args, tsk_id
            blocking: Use blocking enqueue
            
        Returns:
            Number of tasks successfully enqueued
        '''
        count = 0
        for t in tasks:
            success = self.enqueue(
                fn_id=t.get('fn_id'),
                args=t.get('args', ()),
                c_args=t.get('c_args'),
                tsk_id=t.get('tsk_id', 0),
                blocking=blocking,
            )
            if success:
                count += 1
        return count
    
    #==========================================================
    # RUN
    #==========================================================
    def run(self, poll_interval: float = 0.05) -> int:
        '''
        Start workers and run until completion.
        
        Returns:
            Exit code (0=success, 1=interrupted)
        '''
        if self._started:
            raise RuntimeError("run() already called")
        self._started = True
        return self._supervisor.run(poll_interval=poll_interval)
    
    #==========================================================
    # STATUS
    #==========================================================
    def status(self,
               queue: bool = True,
               workers: bool = True,
               config: bool = False,
               functions: bool = False) -> Dict[str, Any]:
        '''
        Get status information.
        
        Args:
            queue: Include queue status
            workers: Include worker status
            config: Include configuration
            functions: Include registered functions
        '''
        result = {}
        
        if config:
            result['config'] = {
                'workers': self._workers,
                'queue_slots': self._queue_slots,
                'slot_class': self._slot_class.__name__,
                'display': self._display,
                'auto_terminate': self._auto_terminate,
                'debug': self._debug,
                'debug_delay': self._debug_delay,
                'validate': self._validate,
            }
        
        if queue:
            q = self._result.queue
            result['queue'] = {
                'occupancy': q.get_actual_occupancy(),
                'capacity': self._queue_slots,
                'debug_counter': q.get_debug_counter(),
                'is_empty': q.is_empty(),
            }
        
        if workers:
            StatusArray = WorkerStatusStruct * 64
            status_array = StatusArray.from_buffer(self._result.status_shm.buf)
            
            workers_list = []
            total_completed = 0
            for i in range(self._workers):
                s = status_array[i]
                total_completed += s.completed_tasks
                workers_list.append({
                    'id': i,
                    'state': STATE_NAMES.get(s.state, "?"),
                    'local': s.local_queue_count,
                    'completed': s.completed_tasks,
                })
            result['workers'] = workers_list
            result['total_completed'] = total_completed
        
        if functions:
            result['functions'] = {
                fn_id: self._fn_names[fn_id]
                for fn_id in self._user_functions
            }
        
        return result
    
    def print_status(self, **kwargs):
        '''Print formatted status.'''
        s = self.status(**kwargs)
        
        print("\n" + "=" * 50)
        print("MPOP STATUS")
        print("=" * 50)
        
        if 'config' in s:
            print("\n[Config]")
            for k, v in s['config'].items():
                print(f"  {k}: {v}")
        
        if 'queue' in s:
            print(f"\n[Queue] occupancy={s['queue']['occupancy']}/{s['queue']['capacity']}, "
                  f"counter={s['queue']['debug_counter']}")
        
        if 'workers' in s:
            print(f"\n[Workers] total_completed={s.get('total_completed', 0)}")
            for w in s['workers']:
                print(f"  W{w['id']}: {w['state']:<10} local={w['local']:<3} done={w['completed']}")
        
        if 'functions' in s:
            print("\n[Functions]")
            for fn_id, name in s['functions'].items():
                print(f"  {fn_id:#06x}: {name}")
        
        print("=" * 50)
    
    #==========================================================
    # CONTEXT MANAGER
    #==========================================================
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
    #==========================================================
    # FACTORY METHODS
    #==========================================================
    @classmethod
    def simple(cls, workers: int = 2, queue_slots: int = 256) -> 'MpopApi':
        '''Create instance without display.'''
        return cls(workers=workers, queue_slots=queue_slots, display=False)
    
    @classmethod
    def debug(cls, workers: int = 2, delay: float = 0.1) -> 'MpopApi':
        '''Create debug instance with delay.'''
        return cls(workers=workers, queue_slots=256, debug=True, debug_delay=delay)


#============================================================
# CONVENIENCE ALIAS
#============================================================
Mpop = MpopApi