# ============================================================
# API/__INIT__.PY
# ============================================================

from .mpop import MpopApi, Mpop

from .slot import TaskSlot, ProcTaskFnID, MAX_ARGS, SLOT_SIZE

from .errors import (
    ErrorCode, Component,
    RegistryError, TypeResolutionError,
    ArgValidationError, FunctionNotFoundError,
)

from .queues import SharedTaskQueue, LocalTaskQueue

from .allocation import allocate, AllocationResult

from .supervisor import SupervisorController

from .worker import (
    WorkerStatusStruct,
    STATE_INIT, STATE_RUNNING, STATE_IDLE, STATE_TERMINATED,
    STATE_NAMES,
)

from .tasks import TaskDispatcher, TaskResult

from .registry import FunctionRegistry, ArgsPool

from .types import (
    HandlerMeta, ParamSpec, StorageMode,
    resolve_handler, classify_type,
)

__all__ = [
    'MpopApi', 'Mpop',
    'TaskSlot', 'ProcTaskFnID', 'MAX_ARGS', 'SLOT_SIZE',
    'ErrorCode', 'Component',
    'RegistryError', 'TypeResolutionError',
    'ArgValidationError', 'FunctionNotFoundError',
    'SharedTaskQueue', 'LocalTaskQueue',
    'allocate', 'AllocationResult',
    'SupervisorController',
    'WorkerStatusStruct',
    'STATE_INIT', 'STATE_RUNNING', 'STATE_IDLE', 'STATE_TERMINATED',
    'STATE_NAMES',
    'TaskDispatcher', 'TaskResult',
    'FunctionRegistry', 'ArgsPool',
    'HandlerMeta', 'ParamSpec', 'StorageMode',
    'resolve_handler', 'classify_type',
]