# ============================================================
# API/REGISTRY.PY — Function registry with ArgsPool
# ============================================================

from typing import Dict, Any, Callable, List, Optional
from multiprocessing import Manager

from .errors import RegistryError, FunctionNotFoundError
from .types import HandlerMeta, resolve_handler
from .slot import MAX_ARGS


# ============================================================
# ARGS POOL — cross-process storage for REFERENCE types
# ============================================================
class ArgsPool:
    """Process-safe pool for REFERENCE type arguments.
    
    LAZY INIT: Manager is only created on first store() call.
    If all handler args are DIRECT types, no Manager process spawns.
    """
    __slots__ = ("_manager", "_pool", "_counter", "_initialized")

    def __init__(self):
        self._manager = None
        self._pool = None
        self._counter = 0
        self._initialized = False

    def _ensure_init(self):
        if not self._initialized:
            self._manager = Manager()
            self._pool = self._manager.dict()
            self._initialized = True

    def store(self, data: Any) -> int:
        """Store data, return pool_id. Main process only."""
        self._ensure_init()
        self._counter += 1
        pool_id = self._counter
        self._pool[pool_id] = data
        return pool_id

    def retrieve(self, pool_id: int) -> Any:
        """Retrieve by pool_id. Worker processes."""
        if not self._initialized:
            return None
        return self._pool.get(pool_id)

    def remove(self, pool_id: int):
        """Remove entry (fire-and-forget cleanup)."""
        if not self._initialized:
            return
        try:
            del self._pool[pool_id]
        except KeyError:
            pass

    @property
    def pool_proxy(self):
        """Manager dict proxy for worker processes. None if not initialized."""
        return self._pool

    def clear(self):
        if self._initialized:
            self._pool.clear()
        self._counter = 0

    def shutdown(self):
        """Shut down Manager process cleanly."""
        if self._manager is not None:
            try:
                self._manager.shutdown()
            except Exception:
                pass
            self._manager = None
            self._pool = None
            self._initialized = False


# ============================================================
# FUNCTION REGISTRY — typed handlers only
# ============================================================
class FunctionRegistry:
    """Stores HandlerMeta for registered handlers. No legacy support."""
    __slots__ = ("_handlers", "_names", "_next_fn_id", "_pool")

    FN_ID_USER_START = 0x1000

    def __init__(self):
        self._handlers: Dict[int, HandlerMeta] = {}
        self._names: Dict[str, int] = {}
        self._next_fn_id = self.FN_ID_USER_START
        self._pool = ArgsPool()

    @property
    def pool(self) -> ArgsPool:
        return self._pool

    def register(self, handler: Callable, fn_id: int = None,
                 name: str = None) -> int:
        """Register a typed handler. Inspects type hints, builds pack/unpack.
        
        All parameters must have type annotations.
        Raises TypeResolutionError on missing/bad hints.
        """
        name = name or handler.__name__

        if fn_id is None:
            fn_id = self._next_fn_id
            self._next_fn_id += 1
        elif fn_id >= self.FN_ID_USER_START:
            if fn_id >= self._next_fn_id:
                self._next_fn_id = fn_id + 1

        if fn_id in self._handlers:
            raise RegistryError(f"fn_id {fn_id:#06x} already registered")
        if name in self._names:
            raise RegistryError(f"Name '{name}' already registered")

        specs = resolve_handler(handler)
        meta = HandlerMeta(
            fn_id=fn_id, name=name, handler=handler,
            specs=specs, pool=self._pool,
        )

        self._handlers[fn_id] = meta
        self._names[name] = fn_id
        return fn_id

    def get(self, fn_id: int) -> Optional[HandlerMeta]:
        return self._handlers.get(fn_id)

    def get_by_name(self, name: str) -> Optional[HandlerMeta]:
        fn_id = self._names.get(name)
        return self._handlers.get(fn_id) if fn_id is not None else None

    def validate(self):
        """Print summary of all registered handlers."""
        handlers = sorted(self._handlers.values(), key=lambda m: m.fn_id)
        if not handlers:
            print("No handlers registered.")
            return

        print("=" * 72)
        print("  REGISTERED HANDLERS")
        print("=" * 72)
        print(f"  {'fn_id':<10} {'name':<20} {'params':<8} {'storage':<16} {'types'}")
        print("-" * 72)
        for meta in handlers:
            n = len(meta.specs)
            storage = meta.storage_summary() if n > 0 else "—"
            types = meta.type_summary() if n > 0 else "—"
            print(f"  {meta.fn_id:#06x}    {meta.name:<20} {n:<8} {storage:<16} {types}")
        print("=" * 72)
        print(f"  Total: {len(handlers)} handlers | Pool active: {self._pool._counter} entries")
        print("=" * 72)

    def list_functions(self) -> List[Dict]:
        return [
            {
                'fn_id': m.fn_id, 'name': m.name,
                'params': len(m.specs),
                'storage': m.storage_summary(),
                'types': m.type_summary(),
                'needs_pool': m.needs_pool,
            }
            for m in sorted(self._handlers.values(), key=lambda m: m.fn_id)
        ]

    def __contains__(self, fn_id: int) -> bool:
        return fn_id in self._handlers

    def __len__(self) -> int:
        return len(self._handlers)