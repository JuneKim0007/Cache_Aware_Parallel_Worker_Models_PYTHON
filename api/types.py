# ============================================================
# API/TYPES.PY — Static type resolution and code generation
# ============================================================
# Classifies handler parameter types at registration time.
# Generates specialized pack/unpack functions with zero branching.
#
# DIRECT types:   fit in 8 bytes → struct.pack into arg slot
# REFERENCE types: don't fit     → ArgsPool → pool_id as uint64
# ============================================================

import struct
import inspect
from enum import IntEnum
from typing import (
    Dict, List, Set, Tuple, Any, Callable, Optional,
    get_type_hints, get_origin, get_args as get_type_args,
)

from .slot import MAX_ARGS
from .errors import TypeResolutionError


# ============================================================
# STORAGE MODE
# ============================================================
class StorageMode(IntEnum):
    DIRECT    = 0   # fits in 8-byte slot
    REFERENCE = 1   # stored in ArgsPool, slot holds pool_id


# ============================================================
# DIRECT TYPE TABLE
# Maps Python type annotations → (struct format, pack_fn, unpack_fn)
# All pack functions: Python value → int64 (for slot storage)
# All unpack functions: int64 → Python value
# ============================================================

# Pre-built struct objects for bit-casting
_S_INT64   = struct.Struct('<q')
_S_UINT64  = struct.Struct('<Q')
_S_FLOAT64 = struct.Struct('<d')
_S_FLOAT32 = struct.Struct('<f')
_S_INT32   = struct.Struct('<i')
_S_UINT32  = struct.Struct('<I')
_S_INT16   = struct.Struct('<h')
_S_UINT16  = struct.Struct('<H')
_S_INT8    = struct.Struct('<b')
_S_UINT8   = struct.Struct('<B')


def _pack_int(v):
    """int → int64 (identity for integers)."""
    return int(v)

def _unpack_int(v):
    """int64 → int (identity)."""
    return int(v)

def _pack_float(v):
    """float → int64 via bit-cast (double → int64)."""
    return _S_INT64.unpack(_S_FLOAT64.pack(float(v)))[0]

def _unpack_float(v):
    """int64 → float via bit-cast (int64 → double)."""
    return _S_FLOAT64.unpack(_S_INT64.pack(v))[0]

def _pack_bool(v):
    """bool → int64."""
    return 1 if v else 0

def _unpack_bool(v):
    """int64 → bool."""
    return v != 0

def _pack_float32(v):
    """float32 → int64 via zero-extended bit-cast."""
    raw = _S_FLOAT32.pack(float(v))
    return _S_INT64.unpack(raw + b'\x00\x00\x00\x00')[0]

def _unpack_float32(v):
    """int64 → float32."""
    raw = _S_INT64.pack(v)[:4]
    return _S_FLOAT32.unpack(raw)[0]

def _pack_int32(v):
    return int(v) & 0xFFFFFFFF if v >= 0 else _S_UINT32.unpack(_S_INT32.pack(int(v)))[0]

def _unpack_int32(v):
    raw = v & 0xFFFFFFFF
    return _S_INT32.unpack(_S_UINT32.pack(raw))[0]

def _pack_uint32(v):
    return int(v) & 0xFFFFFFFF

def _unpack_uint32(v):
    return int(v) & 0xFFFFFFFF

def _pack_int16(v):
    return int(v) & 0xFFFF if v >= 0 else _S_UINT16.unpack(_S_INT16.pack(int(v)))[0]

def _unpack_int16(v):
    raw = v & 0xFFFF
    return _S_INT16.unpack(_S_UINT16.pack(raw))[0]

def _pack_uint16(v):
    return int(v) & 0xFFFF

def _unpack_uint16(v):
    return int(v) & 0xFFFF

def _pack_int8(v):
    return int(v) & 0xFF if v >= 0 else _S_UINT8.unpack(_S_INT8.pack(int(v)))[0]

def _unpack_int8(v):
    raw = v & 0xFF
    return _S_INT8.unpack(_S_UINT8.pack(raw))[0]

def _pack_uint8(v):
    return int(v) & 0xFF

def _unpack_uint8(v):
    return int(v) & 0xFF


# ============================================================
# TYPE REGISTRY — annotation → (StorageMode, pack_fn, unpack_fn, label)
# ============================================================
DIRECT_TYPES: Dict[type, tuple] = {
    int:   (StorageMode.DIRECT, _pack_int,     _unpack_int,     "int64"),
    float: (StorageMode.DIRECT, _pack_float,   _unpack_float,   "float64"),
    bool:  (StorageMode.DIRECT, _pack_bool,    _unpack_bool,    "bool"),
}

# Extended numeric types (optional annotations via string or custom classes)
# Users can reference these via type aliases
EXTENDED_DIRECT: Dict[str, tuple] = {
    "int8":    (StorageMode.DIRECT, _pack_int8,    _unpack_int8,    "int8"),
    "uint8":   (StorageMode.DIRECT, _pack_uint8,   _unpack_uint8,   "uint8"),
    "int16":   (StorageMode.DIRECT, _pack_int16,   _unpack_int16,   "int16"),
    "uint16":  (StorageMode.DIRECT, _pack_uint16,  _unpack_uint16,  "uint16"),
    "int32":   (StorageMode.DIRECT, _pack_int32,   _unpack_int32,   "int32"),
    "uint32":  (StorageMode.DIRECT, _pack_uint32,  _unpack_uint32,  "uint32"),
    "float32": (StorageMode.DIRECT, _pack_float32, _unpack_float32, "float32"),
}


# ============================================================
# PARAMETER SPEC — per-argument metadata
# ============================================================
class ParamSpec:
    """Resolved type information for a single parameter."""
    __slots__ = ("name", "position", "py_type", "storage", "pack_fn", "unpack_fn",
                 "label", "has_default", "default")

    def __init__(self, name: str, position: int, py_type: type,
                 storage: StorageMode, pack_fn: Callable, unpack_fn: Callable,
                 label: str, has_default: bool = False, default: Any = None):
        self.name = name
        self.position = position
        self.py_type = py_type
        self.storage = storage
        self.pack_fn = pack_fn
        self.unpack_fn = unpack_fn
        self.label = label
        self.has_default = has_default
        self.default = default


# ============================================================
# TYPE RESOLVER — classify a type annotation
# ============================================================
def classify_type(annotation: type) -> tuple:
    """Classify annotation → (StorageMode, pack_fn, unpack_fn, label).
    
    Returns DIRECT info for known scalar types, REFERENCE for everything else.
    """
    # Check direct types first
    if annotation in DIRECT_TYPES:
        return DIRECT_TYPES[annotation]
    
    # Check if it's a string name for extended types
    type_name = getattr(annotation, '__name__', str(annotation))
    if type_name in EXTENDED_DIRECT:
        return EXTENDED_DIRECT[type_name]
    
    # Everything else is REFERENCE (str, list, dict, set, tuple, bytes, custom)
    return (StorageMode.REFERENCE, None, None, f"ref<{type_name}>")


# ============================================================
# RESOLVE HANDLER — inspect type hints, build ParamSpec list
# ============================================================
def _get_annotations(handler: Callable) -> Dict:
    """Get type annotations robustly across Python 3.10-3.14+.
    
    Python 3.14 (PEP 749) changed annotation evaluation.
    We try multiple approaches in order of reliability.
    """
    # Approach 1: inspect.get_annotations (3.10+, handles PEP 749)
    try:
        hints = inspect.get_annotations(handler, eval_str=True)
        if hints:
            return hints
    except Exception:
        pass
    
    # Approach 2: typing.get_type_hints (resolves forward refs)
    try:
        hints = get_type_hints(handler)
        if hints:
            return hints
    except Exception:
        pass
    
    # Approach 3: raw __annotations__ dict
    raw = getattr(handler, '__annotations__', None)
    if raw is not None:
        # On 3.14, this may be a lazy proxy — force evaluation
        try:
            return dict(raw)
        except Exception:
            pass
    
    # Approach 4: extract from signature parameters directly
    try:
        sig = inspect.signature(handler)
        hints = {}
        for p in sig.parameters.values():
            if p.annotation is not inspect.Parameter.empty:
                hints[p.name] = p.annotation
        return hints
    except Exception:
        pass
    
    return {}


def resolve_handler(handler: Callable) -> List[ParamSpec]:
    """Inspect handler's type hints and build ParamSpec list.
    
    Enforces: all parameters must have type annotations.
    Raises TypeResolutionError if hints are missing or too many params.
    """
    sig = inspect.signature(handler)
    params = list(sig.parameters.values())
    
    hints = _get_annotations(handler)
    hints.pop('return', None)
    
    if not params:
        return []
    
    if len(params) > MAX_ARGS:
        raise TypeResolutionError(
            f"Handler '{handler.__name__}' has {len(params)} params, max is {MAX_ARGS}"
        )
    
    specs = []
    for i, param in enumerate(params):
        name = param.name
        
        # Check hints dict first, then parameter annotation directly
        annotation = hints.get(name)
        if annotation is None and param.annotation is not inspect.Parameter.empty:
            annotation = param.annotation
        
        if annotation is None:
            raise TypeResolutionError(
                f"Handler '{handler.__name__}': parameter '{name}' missing type annotation. "
                f"All parameters must have type hints."
            )
        
        # Handle string annotations that weren't eval'd
        if isinstance(annotation, str):
            annotation = _resolve_str_annotation(annotation)
        
        storage, pack_fn, unpack_fn, label = classify_type(annotation)
        
        has_default = param.default is not inspect.Parameter.empty
        default = param.default if has_default else None
        
        specs.append(ParamSpec(
            name=name,
            position=i,
            py_type=annotation,
            storage=storage,
            pack_fn=pack_fn,
            unpack_fn=unpack_fn,
            label=label,
            has_default=has_default,
            default=default,
        ))
    
    return specs


def _resolve_str_annotation(s: str) -> type:
    """Resolve string annotation to a type. Handles PEP 563 / 3.14 deferred."""
    _STR_TO_TYPE = {
        'int': int, 'float': float, 'bool': bool,
        'str': str, 'list': list, 'dict': dict,
        'tuple': tuple, 'set': set, 'bytes': bytes,
    }
    return _STR_TO_TYPE.get(s, str)  # unknown string → treat as REFERENCE


# ============================================================
# CODE GENERATION — build specialized pack/unpack functions
# ============================================================

def build_pack_fn(specs: List[ParamSpec], pool) -> Callable:
    """Generate a zero-branch pack function.
    
    Returns: fn(*args) → tuple of int64 values to write into slot.args
    
    For DIRECT types: applies pack_fn (e.g., float → bit-cast → int64)
    For REFERENCE types: stores in pool, returns pool_id
    """
    if not specs:
        def _pack_empty():
            return ()
        return _pack_empty
    
    # Pre-extract into local tuples for fast indexed access
    n = len(specs)
    storages = tuple(s.storage for s in specs)
    pack_fns = tuple(s.pack_fn for s in specs)
    
    # Build the function via exec for zero-overhead argument passing
    # Each arg position gets a specialized line with no branching
    lines = ["def _pack("]
    param_parts = []
    for s in specs:
        if s.has_default:
            param_parts.append(f"  {s.name}=_defaults[{s.position}]")
        else:
            param_parts.append(f"  {s.name}")
    lines.append(",\n".join(param_parts))
    lines.append("):")
    lines.append("  return (")
    
    for i, s in enumerate(specs):
        if s.storage == StorageMode.DIRECT:
            lines.append(f"    _pack_fns[{i}]({s.name}),")
        else:
            lines.append(f"    _pool_store({s.name}),")
    
    lines.append("  )")
    
    code = "\n".join(lines)
    
    # Build namespace
    ns = {
        '_pack_fns': pack_fns,
        '_pool_store': pool.store,
        '_defaults': tuple(s.default for s in specs),
    }
    
    exec(code, ns)
    return ns['_pack']


def build_unpack_fn(specs: List[ParamSpec], pool) -> Callable:
    """Generate a zero-branch unpack function.
    
    Returns: fn(slot_args) → tuple of Python values
    
    For DIRECT types: applies unpack_fn (e.g., int64 → bit-cast → float)
    For REFERENCE types: retrieves from pool by pool_id, then deletes entry
    """
    if not specs:
        def _unpack_empty(slot_args):
            return ()
        return _unpack_empty
    
    n = len(specs)
    unpack_fns = tuple(s.unpack_fn for s in specs)
    
    # Build specialized function
    lines = ["def _unpack(slot_args):"]
    lines.append("  return (")
    
    for i, s in enumerate(specs):
        if s.storage == StorageMode.DIRECT:
            lines.append(f"    _unpack_fns[{i}](slot_args[{i}]),")
        else:
            lines.append(f"    _pool_pop({i}, slot_args[{i}]),")
    
    lines.append("  )")
    
    code = "\n".join(lines)
    
    def _pool_pop(idx, pool_id):
        """Retrieve and remove from pool (fire-and-forget cleanup)."""
        val = pool.retrieve(pool_id)
        pool.remove(pool_id)
        return val
    
    ns = {
        '_unpack_fns': unpack_fns,
        '_pool_pop': _pool_pop,
    }
    
    exec(code, ns)
    return ns['_unpack']


def build_positional_pack_fn(specs: List[ParamSpec], pool) -> Callable:
    """Generate pack function for positional args tuple.
    
    Returns: fn(args_tuple) → tuple of int64 values
    Used by app.enqueue(fn_id=0x8000, args=(10, 3.14))
    """
    if not specs:
        def _pack_pos(args):
            return ()
        return _pack_pos
    
    n = len(specs)
    
    lines = ["def _pack_pos(args):"]
    lines.append("  return (")
    
    for i, s in enumerate(specs):
        if s.storage == StorageMode.DIRECT:
            lines.append(f"    _pack_fns[{i}](args[{i}]),")
        else:
            lines.append(f"    _pool_store(args[{i}]),")
    
    lines.append("  )")
    
    code = "\n".join(lines)
    ns = {
        '_pack_fns': tuple(s.pack_fn for s in specs),
        '_pool_store': pool.store,
    }
    
    exec(code, ns)
    return ns['_pack_pos']


# ============================================================
# HANDLER META — complete registration-time metadata
# ============================================================
class HandlerMeta:
    """All metadata for a registered handler. Built once at registration."""
    __slots__ = (
        "fn_id", "name", "handler", "specs",
        "pack_fn",           # fn(**kwargs) → tuple of int64
        "pack_positional_fn", # fn(args_tuple) → tuple of int64
        "unpack_fn",         # fn(slot_args) → tuple of Python values
        "param_names",       # tuple of param names for kwarg matching
        "needs_pool",        # True if any REFERENCE args
    )
    
    def __init__(self, fn_id: int, name: str, handler: Callable,
                 specs: List[ParamSpec], pool):
        self.fn_id = fn_id
        self.name = name
        self.handler = handler
        self.specs = specs
        self.param_names = tuple(s.name for s in specs)
        self.needs_pool = any(s.storage == StorageMode.REFERENCE for s in specs)
        
        # Generate specialized functions
        self.pack_fn = build_pack_fn(specs, pool)
        self.pack_positional_fn = build_positional_pack_fn(specs, pool)
        self.unpack_fn = build_unpack_fn(specs, pool)
    
    def storage_summary(self) -> str:
        """Human-readable storage mode list."""
        modes = []
        for s in self.specs:
            modes.append("D" if s.storage == StorageMode.DIRECT else "R")
        return "[" + ", ".join(modes) + "]"
    
    def type_summary(self) -> str:
        """Human-readable type list."""
        return ", ".join(f"{s.name}:{s.label}" for s in self.specs)