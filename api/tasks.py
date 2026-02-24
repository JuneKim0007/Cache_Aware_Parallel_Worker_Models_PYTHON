# ============================================================
# API/TASKS.PY — Task dispatch (typed handlers only)
# ============================================================

from typing import Callable, Dict, Any

from .slot import ProcTaskFnID
from .types import HandlerMeta


class TaskResult:
    """Internal task execution result. Used to validate handler success."""
    __slots__ = ("success", "value", "error")

    def __init__(self, success: bool, value: Any = None, error: str = None):
        self.success = success
        self.value = value
        self.error = error


# Pre-allocated singleton
_RESULT_OK = TaskResult(success=True)


# ============================================================
# BUILTIN HANDLERS (typed — all params annotated)
# ============================================================
def _handle_increment(a: int, b: int) -> int:
    return a + b

def _handle_add(a: int, b: int) -> int:
    return a + b

def _handle_multiply(a: int, b: int) -> int:
    return a * b


# ============================================================
# TASK DISPATCHER — typed handlers only
# ============================================================
class TaskDispatcher:
    """Routes fn_id → HandlerMeta → unpack → handler call.
    
    All handlers are typed. No legacy (slot, ctx) path.
    TERMINATE is handled inline by the worker loop, not here.
    """
    __slots__ = ("_handlers",)

    def __init__(self):
        self._handlers: Dict[int, HandlerMeta] = {}

    def register(self, meta: HandlerMeta):
        """Register a typed handler."""
        self._handlers[meta.fn_id] = meta

    def dispatch(self, slot) -> TaskResult:
        """Dispatch a task. Zero-branch per argument on hot path."""
        fn_id = slot.fn_id

        meta = self._handlers.get(fn_id)
        if meta is None:
            return TaskResult(success=False, error=f"Unknown fn_id {fn_id:#06x}")

        try:
            args = meta.unpack_fn(slot.args)
            result = meta.handler(*args)
            # Auto-wrap plain returns into TaskResult
            if isinstance(result, TaskResult):
                return result
            return TaskResult(success=True, value=result)
        except Exception as e:
            return TaskResult(success=False, error=str(e))