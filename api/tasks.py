# ============================================================
# API/TASKS.PY
# ============================================================
# Change log: REPLACED Dict_based fetching to Tuple based fetching
#             Also, added __slot__() attribute to reduce memory pressure
#             Removed my personal comments

from typing import Callable, Dict, Any
from .slot import ProcTaskFnID
from .args import ArgParser


class TaskResult:
    __slots__= ("success", "value", "error")

    def __init__(self, success: bool, value: Any = None, error: str = None):
        self.success = success
        self.value = value
        self.error = error

class TaskContext:
    __slots__= ("worker_id", "arg_parser", "log_func", "extra")

    def __init__(self,
                 worker_id: int,
                 arg_parser: ArgParser = None,
                 log_func: Callable[[str], None] = None,
                 extra: Dict[str, Any] = None):
        self.worker_id = worker_id
        self.arg_parser = arg_parser
        self.log_func = log_func
        self.extra = extra

_RESULT_TERMINATE = TaskResult(success=True, value="TERMINATE")
_RESULT_OK = TaskResult(success=True)


def handle_terminate(slot, ctx: TaskContext) -> TaskResult:
    return _RESULT_TERMINATE


def handle_increment(slot, ctx: TaskContext) -> TaskResult:
    a = slot.args[0]
    b = slot.args[1] if len(slot.args) > 1 else 1
    result = a + b
    if ctx.log_func:
        ctx.log_func(f"INCREMENT: {a} + {b} = {result}")
    return TaskResult(success=True, value=result)


def handle_add(slot, ctx: TaskContext) -> TaskResult:
    return TaskResult(success=True, value=slot.args[0] + slot.args[1])


def handle_multiply(slot, ctx: TaskContext) -> TaskResult:
    return TaskResult(success=True, value=slot.args[0] * slot.args[1])


def handle_status_report(slot, ctx: TaskContext) -> TaskResult:
    if ctx.log_func:
        ctx.log_func(f"STATUS: worker_id={ctx.worker_id}")
    return _RESULT_OK


def handle_default(slot, ctx: TaskContext) -> TaskResult:
    return _RESULT_OK


_BUILTIN_HANDLERS: Dict[int, Callable] = {
    ProcTaskFnID.TERMINATE: handle_terminate,
    ProcTaskFnID.INCREMENT: handle_increment,
    ProcTaskFnID.ADD: handle_add,
    ProcTaskFnID.MULTIPLY: handle_multiply,
    ProcTaskFnID.STATUS_REPORT: handle_status_report,
}


# ============================================================
# TASK DISPATCHER
# ============================================================
class TaskDispatcher:
    __slots__ = ("_handlers", "_default")

    def __init__(self):
        self._handlers: Dict[int, Callable] = dict(_BUILTIN_HANDLERS)
        self._default = handle_default

    def register(self, fn_id: int, handler: Callable):
        self._handlers[fn_id] = handler

    def dispatch(self, slot, ctx: TaskContext) -> TaskResult:
        handler = self._handlers.get(slot.fn_id, self._default)
        try:
            return handler(slot, ctx)
        except Exception as e:
            return TaskResult(success=False, error=str(e))