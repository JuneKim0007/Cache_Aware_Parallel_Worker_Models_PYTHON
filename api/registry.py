# ============================================================
# API/REGISTRY.PY
# ============================================================
import ctypes
from typing import Dict, Any, Callable, List, Union, Optional, Tuple
from multiprocessing import Manager

from .errors import RegistryError, ArgValidationError, FunctionNotFoundError


class FunctionEntry:
    """Registered function metadata. __slots__ — no __dict__."""
    __slots__ = ("fn_id", "name", "handler", "arg_count", "c_arg_max", "meta")

    def __init__(self,
                 fn_id: int,
                 name: str,
                 handler: Callable,
                 arg_count: int = 0,
                 c_arg_max: int = 64,
                 meta: Dict[str, Any] = None):
        self.fn_id = fn_id
        self.name = name
        self.handler = handler
        self.arg_count = arg_count
        self.c_arg_max = c_arg_max
        self.meta = meta if meta is not None else {}


# ============================================================
# ARGS POOL
# ============================================================
class ArgsPool:
    __slots__ = ("_use_manager", "_manager", "_pool", "_vars", "_next_id")

    def __init__(self, use_manager: bool = True):
        self._use_manager = use_manager
        if use_manager:
            self._manager = Manager()
            self._pool: Dict[int, Any] = self._manager.dict()
            self._vars: Dict[str, Any] = self._manager.dict()
        else:
            self._manager = None
            self._pool: Dict[int, Any] = {}
            self._vars: Dict[str, Any] = {}
        self._next_id = 1

    def store(self, data: Any) -> int:
        pool_id = self._next_id
        self._next_id += 1
        self._pool[pool_id] = data
        return pool_id

    def retrieve(self, pool_id: int) -> Any:
        return self._pool.get(pool_id)

    def remove(self, pool_id: int):
        if pool_id in self._pool:
            del self._pool[pool_id]

    def set_var(self, name: str, value: Any):
        self._vars[name] = value

    def get_var(self, name: str) -> Any:
        return self._vars.get(name)

    def has_var(self, name: str) -> bool:
        return name in self._vars

    def clear(self):
        self._pool.clear()
        self._vars.clear()
        self._next_id = 1


# ============================================================
# FUNCTION REGISTRY
# ============================================================
class FunctionRegistry:
    __slots__ = (
        "_functions", "_names", "_next_fn_id",
        "_slot_int_args", "_slot_c_args", "_delimiter",
        "_pool",
    )
    #changed since i dont think i will have half of the space for internal task allocations
    FN_ID_USER_START = 0x1000
    FN_ID_SYSTEM_END = 0x0FFF

    def __init__(self,
                 slot_int_args: int = 2,
                 slot_c_args: int = 64,
                 delimiter: str = ' '):
        self._functions: Dict[int, FunctionEntry] = {}
        self._names: Dict[str, int] = {}
        self._next_fn_id = self.FN_ID_USER_START
        self._slot_int_args = slot_int_args
        self._slot_c_args = slot_c_args
        self._delimiter = delimiter
        self._pool = ArgsPool(use_manager=True)

    @property
    def delimiter(self) -> str:
        return self._delimiter

    @delimiter.setter
    def delimiter(self, value: str):
        self._delimiter = value

    @property
    def pool(self) -> ArgsPool:
        return self._pool

    def register(self,
                 handler: Callable,
                 name: str = None,
                 fn_id: int = None,
                 arg_count: int = 0,
                 c_arg_max: int = None,
                 meta: Dict = None) -> int:
        name = name or handler.__name__

        if fn_id is None:
            fn_id = self._next_fn_id
            self._next_fn_id += 1

        if fn_id in self._functions:
            raise RegistryError(f"fn_id {fn_id:#06x} already registered")
        if name in self._names:
            raise RegistryError(f"Name '{name}' already registered")

        entry = FunctionEntry(
            fn_id=fn_id,
            name=name,
            handler=handler,
            arg_count=arg_count,
            c_arg_max=c_arg_max or self._slot_c_args,
            meta=meta,
        )
        self._functions[fn_id] = entry
        self._names[name] = fn_id
        return fn_id

    def get(self, fn_id: int) -> Optional[FunctionEntry]:
        return self._functions.get(fn_id)

    def get_by_name(self, name: str) -> Optional[FunctionEntry]:
        fn_id = self._names.get(name)
        return self._functions.get(fn_id) if fn_id is not None else None

    def get_handler(self, fn_id: int) -> Optional[Callable]:
        entry = self._functions.get(fn_id)
        return entry.handler if entry is not None else None

    def set_var(self, name: str, value: Any):
        self._pool.set_var(name, value)

    def get_var(self, name: str) -> Any:
        return self._pool.get_var(name)

    def set_shared(self, name: str, value: Any):
        self._pool.set_var(f"shared:{name}", value)

    def get_shared(self, name: str) -> Any:
        return self._pool.get_var(f"shared:{name}")

    def has_shared(self, name: str) -> bool:
        return self._pool.has_var(f"shared:{name}")

    def list_shared(self) -> List[str]:
        prefix = "shared:"
        prefix_len = len(prefix)
        return [k[prefix_len:] for k in self._pool._vars if isinstance(k, str) and k.startswith(prefix)]

    def validate_args(self,
                      fn_id: int,
                      args: tuple,
                      c_args: Union[bytes, str, List[str]] = None) -> Tuple[bool, str]:
        if len(args) > self._slot_int_args:
            return False, f"Too many int args: {len(args)} > {self._slot_int_args}"

        entry = self._functions.get(fn_id)
        if entry is not None and entry.arg_count > 0 and len(args) != entry.arg_count:
            return False, f"Expected {entry.arg_count} args, got {len(args)}"

        return True, ""

    def prepare_args(self,
                     fn_id: int,
                     args: tuple = (),
                     c_args: Union[bytes, str, List[str]] = None
                     ) -> Tuple[tuple, bytes, int]:
        valid, err = self.validate_args(fn_id, args, c_args)
        if not valid:
            raise ArgValidationError(err)

        pool_id = 0
        packed_c_args = b''

        if c_args is not None:
            packed_c_args, pool_id = self._process_c_args(c_args)

        return args, packed_c_args, pool_id

    def _process_c_args(self, c_args: Union[bytes, str, List[str]]) -> Tuple[bytes, int]:
        pool_id = 0

        if isinstance(c_args, str) and c_args.startswith('var:'):
            var_name = c_args[4:]
            value = self._pool.get_var(var_name)
            if value is None:
                raise ArgValidationError(f"Variable '{var_name}' not found")
            pool_id = self._pool.store(value)
            packed = f"pool:{pool_id}".encode('utf-8') + b'\x00'
            return packed, pool_id

        if isinstance(c_args, str):
            packed = c_args.encode('utf-8') + b'\x00'
        elif isinstance(c_args, list):
            delim = self._delimiter.encode('utf-8')
            packed = delim.join(s.encode('utf-8') for s in c_args) + b'\x00'
        else:
            packed = c_args

        #FLush to pool
        if len(packed) > self._slot_c_args:
            pool_id = self._pool.store(packed)
            packed = f"pool:{pool_id}".encode('utf-8') + b'\x00'

        return packed, pool_id

    def resolve_c_args(self, c_args: bytes) -> Any:
        try:
            text = c_args.rstrip(b'\x00').decode('utf-8')
            if text.startswith('pool:'):
                pool_id = int(text[5:])
                return self._pool.retrieve(pool_id)
        except Exception:
            pass
        return c_args

    def dispatch(self, fn_id: int, slot, ctx) -> Any:
        handler = self.get_handler(fn_id)
        if handler is None:
            raise FunctionNotFoundError(f"fn_id {fn_id:#06x} not registered")
        return handler(slot, ctx)

    def list_functions(self) -> List[Dict]:
        return [
            {
                'fn_id': e.fn_id,
                'name': e.name,
                'arg_count': e.arg_count,
                'c_arg_max': e.c_arg_max,
            }
            for e in self._functions.values()
        ]

    def __contains__(self, fn_id: int) -> bool:
        return fn_id in self._functions

    def __len__(self) -> int:
        return len(self._functions)