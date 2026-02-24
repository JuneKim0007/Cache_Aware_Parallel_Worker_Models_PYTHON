# ============================================================
# API/ERRORS.PY — Consolidated error definitions
# ============================================================

from enum import IntEnum
from typing import Optional


class Component:
    __slots__ = ()
    SHARED_QUEUE = "SharedQueue"
    LOCAL_QUEUE  = "LocalQueue"
    WORKER       = "Worker"
    ALLOCATION   = "Allocation"
    SUPERVISOR   = "Supervisor"
    REGISTRY     = "Registry"


class ErrorCode(IntEnum):
    E001_QUEUE_FULL          = 1
    E002_INVALID_NUM_SLOTS   = 2
    E003_NUM_WORKERS_EXCEEDED = 3
    E004_INVALID_BATCH_SIZE  = 4
    E005_QUEUE_EMPTY         = 5
    E011_INVALID_CONSUMER_ID = 11
    E012_SHARED_QUEUE_NONE   = 12
    E021_SHM_ALLOC_FAILED    = 21
    E022_PROCESS_SPAWN_FAILED = 22
    E023_INVALID_SLOT_CLASS  = 23
    E030_TYPE_ERROR          = 30
    E031_REGISTRATION_ERROR  = 31


class RegistryError(Exception):
    __slots__ = ()

class ArgValidationError(RegistryError):
    __slots__ = ()

class FunctionNotFoundError(RegistryError):
    __slots__ = ()

class TypeResolutionError(RegistryError):
    __slots__ = ()


def format_error(code: ErrorCode, component: str, message: str) -> str:
    return f"[Error: E{code:03d}][{component}] {message}"


def validate_num_slots(num_slots: int) -> Optional[str]:
    if num_slots <= 0:
        return f"num_slots must be positive, got {num_slots}"
    if num_slots & (num_slots - 1) != 0:
        return f"num_slots must be power of 2, got {num_slots}"
    return None


def validate_num_workers(num_workers: int) -> Optional[str]:
    if num_workers <= 0:
        return f"num_workers must be positive, got {num_workers}"
    if num_workers > 64:
        return f"num_workers must be <= 64, got {num_workers}"
    return None