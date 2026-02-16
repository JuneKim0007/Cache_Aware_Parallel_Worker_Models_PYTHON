# ============================================================
# API/__INIT__.PY
# ============================================================

from .mpop import (
    MpopApi,
    Mpop,
    ValidationError,
)

from .slot import (
    TaskSlot128,
    TaskSlot196,
    TaskSlot128_cargs,
    TaskSlot196_cargs,
    ProcTaskFnID,
    SlotVariant,
)

from .errors import (
    ErrorCode,
    Component,
    RegistryError,
    ArgValidationError,
    FunctionNotFoundError,
)

from .queues import (
    SharedTaskQueue,
    LocalTaskQueue,
)

from .allocation import (
    allocate,
    cleanup,
    AllocationResult,
)

from .supervisor import (
    SupervisorController,
)

from .worker import (
    WorkerStatusStruct,
    STATE_INIT,
    STATE_RUNNING,
    STATE_IDLE,
    STATE_TERMINATED,
    STATE_NAMES,
)

from .tasks import (
    TaskDispatcher,
    TaskResult,
    TaskContext,
)

from .args import (
    ArgParser,
    unpack_args,
)

from .registry import (
    FunctionRegistry,
    ArgsPool,
    FunctionEntry,
)

__all__ = [
    # Primary
    'MpopApi', 'Mpop', 'ValidationError',
    # Slots
    'TaskSlot128', 'TaskSlot196', 'TaskSlot128_cargs', 'TaskSlot196_cargs',
    'ProcTaskFnID', 'SlotVariant',
    # Errors (single source)
    'ErrorCode', 'Component',
    'RegistryError', 'ArgValidationError', 'FunctionNotFoundError',
    # Registry
    'FunctionRegistry', 'ArgsPool', 'FunctionEntry',
    # Internal
    'SharedTaskQueue', 'LocalTaskQueue',
    'allocate', 'cleanup', 'AllocationResult',
    'SupervisorController',
    'WorkerStatusStruct', 'STATE_INIT', 'STATE_RUNNING', 'STATE_IDLE', 'STATE_TERMINATED', 'STATE_NAMES',
    'TaskDispatcher', 'TaskResult', 'TaskContext',
    'ArgParser', 'unpack_args',
]