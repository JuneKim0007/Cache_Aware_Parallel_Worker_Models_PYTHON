# ============================================================
# TASKS.PY
# ============================================================

import ctypes
from typing import Callable, Dict, Any, Tuple, Optional
from enum import IntEnum
from slot import ProcTaskFnID, TaskSlot128, SlotVariant
from args import ArgParser, unpack_args


#============================================================
# TASK RESULT
#============================================================
class TaskResult:
    '''Result from task execution.'''
    __slots__ = ('success', 'value', 'error')
    
    def __init__(self, success: bool, value: Any = None, error: str = None):
        self.success = success
        self.value = value
        self.error = error
    
    def __repr__(self):
        if self.success:
            return f"TaskResult(OK, value={self.value})"
        return f"TaskResult(FAIL, error={self.error})"


#============================================================
# TASK CONTEXT
#============================================================
class TaskContext:
    __slots__ = ('worker_id', 'arg_parser', 'log_func', 'arg_pool', 'extra')
    def __init__(self, 
                 worker_id: int,
                 arg_parser: ArgParser = None,
                 log_func: Callable = None,
                 arg_pool = None,
                 extra: Dict = None):
        self.worker_id = worker_id
        self.arg_parser = arg_parser or ArgParser()
        self.log_func = log_func or (lambda msg: None)
        self.arg_pool = arg_pool
        self.extra = extra or {}
    
    def log(self, message: str):
        '''Log message through worker's log function.'''
        self.log_func(message)
    
    def parse_cargs(self, c_args: bytes) -> list:
        '''Parse c_args using configured parser.'''
        return self.arg_parser.parse_as_str(c_args)


#============================================================
# TASK HANDLER TYPE
#============================================================
#Handler signature: (slot, context) -> TaskResult
TaskHandler = Callable[[Any, TaskContext], TaskResult]


#============================================================
# BUILT-IN TASK HANDLERS
# RIGHT NOW MOSTLY USED FOR DEBUGGING
#============================================================

def handle_terminate(slot, ctx: TaskContext) -> TaskResult:
    ctx.log(f"TERMINATE received tsk_id={slot.tsk_id}")
    return TaskResult(success=True, value="terminated")


def handle_increment(slot, ctx: TaskContext) -> TaskResult:
    value = slot.args[0] if slot.args[0] != 0 else 0
    increment = slot.args[1] if slot.args[1] != 0 else 1
    result = value + increment
    
    ctx.log(f"INCREMENT: {value} + {increment} = {result}")
    return TaskResult(success=True, value=result)


def handle_add(slot, ctx: TaskContext) -> TaskResult:
    a = slot.args[0]
    b = slot.args[1]
    result = a + b
    
    ctx.log(f"ADD: {a} + {b} = {result}")
    return TaskResult(success=True, value=result)


def handle_multiply(slot, ctx: TaskContext) -> TaskResult:
    a = slot.args[0]
    b = slot.args[1]
    result = a * b
    
    ctx.log(f"MULTIPLY: {a} * {b} = {result}")
    return TaskResult(success=True, value=result)


def handle_hash(slot, ctx: TaskContext) -> TaskResult:
    import hashlib
    
    data = b''
    for i in range(10):  #TaskSlot128 has 10 args
        if slot.args[i] != 0:
            data += slot.args[i].to_bytes(8, 'little', signed=True)
    
    if not data:
        data = b'empty'
    
    hash_val = hashlib.md5(data).hexdigest()[:8]
    ctx.log(f"HASH: {hash_val}")
    return TaskResult(success=True, value=hash_val)


#### SIMPLE EXAMPLE TASKS THAT PRATICALLY DO NOTHING BUT FLUSH 
def handle_echo_cargs(slot, ctx: TaskContext) -> TaskResult:
    if not hasattr(slot, 'c_args'):
        return TaskResult(success=False, error="Slot has no c_args field")
    
    args = ctx.parse_cargs(bytes(slot.c_args))
    ctx.log(f"ECHO: {args}")
    return TaskResult(success=True, value=args)


def handle_status_report(slot, ctx: TaskContext) -> TaskResult:
    '''Generate status report.'''
    ctx.log(f"STATUS: worker={ctx.worker_id}, tsk_id={slot.tsk_id}")
    return TaskResult(success=True, value={"worker_id": ctx.worker_id})


def handle_flush(slot, ctx: TaskContext) -> TaskResult:
    ctx.log(f"FLUSH: tsk_id={slot.tsk_id} fn_id={slot.fn_id:#06x}")
    return TaskResult(success=True)


#============================================================
# TASK DISPATCHER
#============================================================
class TaskDispatcher:
    '''
    Maps fn_id to handler functions.
    Workers use this to dispatch tasks.
    '''
    
    def __init__(self):
        self._handlers: Dict[int, TaskHandler] = {}
        self._default_handler: TaskHandler = handle_flush
        
        #Register built-in handlers
        self._register_builtins()
    
    def _register_builtins(self):
        '''Register built-in task handlers.'''
        self.register(ProcTaskFnID.TERMINATE, handle_terminate)
        self.register(ProcTaskFnID.INCREMENT, handle_increment)
        self.register(ProcTaskFnID.HASH, handle_hash)
        self.register(ProcTaskFnID.STATUS_REPORT, handle_status_report)
        
        #Extended handlers (using user range)
        self.register(0x2200, handle_add)
        self.register(0x2300, handle_multiply)
        self.register(0x3000, handle_echo_cargs)
    
    def register(self, fn_id: int, handler: TaskHandler):
        '''Register a handler for fn_id.'''
        self._handlers[fn_id] = handler
    
    def unregister(self, fn_id: int):
        '''Remove handler for fn_id.'''
        self._handlers.pop(fn_id, None)
    
    def set_default(self, handler: TaskHandler):
        '''Set default handler for unknown fn_ids.'''
        self._default_handler = handler
    
    def dispatch(self, slot, ctx: TaskContext) -> TaskResult:
        '''
        Dispatch task to appropriate handler.
        
        Args:
            slot: Task slot
            ctx: Task context
            
        Returns:
            TaskResult from handler
        '''
        handler = self._handlers.get(slot.fn_id, self._default_handler)
        
        try:
            return handler(slot, ctx)
        except Exception as e:
            return TaskResult(success=False, error=str(e))
    
    def get_handler(self, fn_id: int) -> Optional[TaskHandler]:
        '''Get handler for fn_id, or None if not registered.'''
        return self._handlers.get(fn_id)
    
    def list_handlers(self) -> Dict[int, str]:
        '''List all registered handlers with their names.'''
        return {fn_id: h.__name__ for fn_id, h in self._handlers.items()}


#============================================================
# GLOBAL DISPATCHER INSTANCE
#============================================================
_default_dispatcher = None

def get_dispatcher() -> TaskDispatcher:
    '''Get the global task dispatcher instance.'''
    global _default_dispatcher
    if _default_dispatcher is None:
        _default_dispatcher = TaskDispatcher()
    return _default_dispatcher


def register_handler(fn_id: int, handler: TaskHandler):
    '''Register handler with global dispatcher.'''
    get_dispatcher().register(fn_id, handler)


def dispatch_task(slot, ctx: TaskContext) -> TaskResult:
    '''Dispatch task using global dispatcher.'''
    return get_dispatcher().dispatch(slot, ctx)


#============================================================
# HELPER: CREATE TASK SLOT
#============================================================
def create_task(tsk_id: int, 
                fn_id: int,
                args: list = None,
                slot_class = TaskSlot128) -> Any:
    '''
    Helper to create a task slot with given parameters.
    
    Args:
        tsk_id: Task ID
        fn_id: Function ID
        args: List of integer arguments (up to 10)
        slot_class: Slot class to use
        
    Returns:
        Populated slot instance
    '''
    slot = slot_class()
    slot.tsk_id = tsk_id
    slot.fn_id = fn_id
    
    if args:
        for i, val in enumerate(args[:10]):
            slot.args[i] = val
    
    return slot


def create_cargs_task(tsk_id: int,
                      fn_id: int,
                      c_args: str,
                      int_args: list = None) -> Any:
    '''
    Helper to create a CHAR_ARGS task slot.
    
    Args:
        tsk_id: Task ID
        fn_id: Function ID
        c_args: String arguments (will be null-terminated)
        int_args: Optional integer arguments (up to 2)
        
    Returns:
        Populated TaskSlot128_cargs instance
    '''
    from slot import TaskSlot128_cargs
    
    slot = TaskSlot128_cargs()
    slot.tsk_id = tsk_id
    slot.fn_id = fn_id
    
    #Pack c_args
    c_bytes = c_args.encode('utf-8') + b'\x00'
    slot.c_args = c_bytes[:64]  #Truncate if too long
    
    if int_args:
        for i, val in enumerate(int_args[:2]):
            slot.args[i] = val
    
    return slot