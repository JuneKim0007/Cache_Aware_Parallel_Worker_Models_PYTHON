# ============================================================
# API/__INIT__.PY
# ============================================================
# Public API exports.
#
# Primary entry point: MpopApi (alias: Mpop)
# Internal/Advanced: allocate, SharedTaskQueue, etc.
# ============================================================

#----------------------------------------------------------
# PRIMARY USER API
#----------------------------------------------------------
from .mpop import (
    MpopApi,
    Mpop,
    ValidationError,
)

#----------------------------------------------------------
# SLOTS
#----------------------------------------------------------
from .slot import (
    TaskSlot128,
    TaskSlot196,
    TaskSlot128_cargs,
    TaskSlot196_cargs,
    ProcTaskFnID,
    SlotVariant,
)

#----------------------------------------------------------
# ADVANCED / INTERNAL
#----------------------------------------------------------
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
)

from .registry import (
    FunctionRegistry,
    ArgsPool,
    FunctionEntry,
    RegistryError,
    ArgValidationError,
    FunctionNotFoundError,
)

from .errors import (
    ErrorCode,
    Component,
)

__all__ = [
    #Primary
    'MpopApi', 'Mpop', 'ValidationError',
    #Slots
    'TaskSlot128', 'TaskSlot196', 'TaskSlot128_cargs', 'TaskSlot196_cargs',
    'ProcTaskFnID', 'SlotVariant',
    #Registry
    'FunctionRegistry', 'ArgsPool', 'FunctionEntry',
    'RegistryError', 'ArgValidationError', 'FunctionNotFoundError',
    #Advanced
    'SharedTaskQueue', 'LocalTaskQueue',
    'allocate', 'cleanup', 'AllocationResult',
    'SupervisorController',
    'WorkerStatusStruct', 'STATE_INIT', 'STATE_RUNNING', 'STATE_IDLE', 'STATE_TERMINATED', 'STATE_NAMES',
    'TaskDispatcher', 'TaskResult', 'TaskContext',
    'ArgParser',
    'ErrorCode', 'Component',
]