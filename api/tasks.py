# ============================================================
# API/TASKS.PY
# ============================================================
# THIS TASKS.PY IS MAINLY TO STORE INTERNAL AND NECESSARY TASKS 
# SOME TASKS LIKE HASHING ARE UNNNECESSRAY BUT THERE FOR TEMPORAL USE.

from typing import Callable, Dict, Any, Optional
from dataclasses import dataclass

from .slot import ProcTaskFnID
from .args import ArgParser


@dataclass
class TaskResult:
    success: bool
    value: Any = None
    error: str = None


@dataclass
class TaskContext:
    worker_id: int
    arg_parser: ArgParser = None
    log_func: Callable[[str], None] = None
    extra: Dict[str, Any] = None


#============================================================
# BUILT-IN HANDLERS
# some of them are used for testing purposes, will be removed later
#============================================================
def handle_terminate(slot, ctx: TaskContext) -> TaskResult:
    return TaskResult(success=True, value="TERMINATE")


def handle_increment(slot, ctx: TaskContext) -> TaskResult:
    a = slot.args[0]
    b = slot.args[1] if len(slot.args) > 1 else 1
    result = a + b
    if ctx.log_func:
        ctx.log_func(f"INCREMENT: {a} + {b} = {result}")
    return TaskResult(success=True, value=result)


def handle_add(slot, ctx: TaskContext) -> TaskResult:
    a, b = slot.args[0], slot.args[1]
    return TaskResult(success=True, value=a + b)


def handle_multiply(slot, ctx: TaskContext) -> TaskResult:
    a, b = slot.args[0], slot.args[1]
    return TaskResult(success=True, value=a * b)


def handle_status_report(slot, ctx: TaskContext) -> TaskResult:
    if ctx.log_func:
        ctx.log_func(f"STATUS: worker_id={ctx.worker_id}")
    return TaskResult(success=True)


def handle_default(slot, ctx: TaskContext) -> TaskResult:
    return TaskResult(success=True)


class TaskDispatcher:
    def __init__(self):
        self._handlers: Dict[int, Callable] = {
            ProcTaskFnID.TERMINATE: handle_terminate,
            ProcTaskFnID.INCREMENT: handle_increment,
            ProcTaskFnID.ADD: handle_add,
            ProcTaskFnID.MULTIPLY: handle_multiply,
            ProcTaskFnID.STATUS_REPORT: handle_status_report,
        }
        self._default = handle_default
    
    def register(self, fn_id: int, handler: Callable):
        self._handlers[fn_id] = handler
    
    def dispatch(self, slot, ctx: TaskContext) -> TaskResult:
        handler = self._handlers.get(slot.fn_id, self._default)
        try:
            return handler(slot, ctx)
        except Exception as e:
            return TaskResult(success=False, error=str(e))