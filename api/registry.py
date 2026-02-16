# ============================================================
# API/REGISTRY.PY
# ============================================================

import ctypes
from typing import Dict, Any, Callable, List, Union, Optional, Tuple
from multiprocessing import Manager
from dataclasses import dataclass, field


#============================================================
# ERRORS
#============================================================
class RegistryError(Exception):
    '''Base registry error.'''
    pass

class ArgValidationError(RegistryError):
    '''Argument validation failed.'''
    pass

class FunctionNotFoundError(RegistryError):
    '''Function ID not registered.'''
    pass


#============================================================
# FUNCTION ENTRY
#============================================================
@dataclass
class FunctionEntry:
    '''Registered function metadata.'''
    fn_id: int
    name: str
    handler: Callable
    arg_count: int = 0          # Expected int args count (0 = any)
    c_arg_max: int = 64         # Max c_args bytes (overflow goes to pool)
    meta: Dict[str, Any] = field(default_factory=dict)


#============================================================
# ARGS POOL
#============================================================
class ArgsPool:
    
    def __init__(self, use_manager: bool = True):
        '''
        Args:
            use_manager: Use multiprocessing.Manager for cross-process access
        '''
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
        '''Store data in pool, return pool_id.'''
        pool_id = self._next_id
        self._next_id += 1
        self._pool[pool_id] = data
        return pool_id
    
    def retrieve(self, pool_id: int) -> Any:
        '''Retrieve data from pool.'''
        return self._pool.get(pool_id)
    
    def remove(self, pool_id: int):
        '''Remove data from pool after use.'''
        if pool_id in self._pool:
            del self._pool[pool_id]
    
    def set_var(self, name: str, value: Any):
        '''Set a named variable.'''
        self._vars[name] = value
    
    def get_var(self, name: str) -> Any:
        '''Get a named variable.'''
        return self._vars.get(name)
    
    def has_var(self, name: str) -> bool:
        '''Check if variable exists.'''
        return name in self._vars
    
    def clear(self):
        '''Clear all pool data and variables.'''
        self._pool.clear()
        self._vars.clear()
        self._next_id = 1


#============================================================
# FUNCTION REGISTRY
#============================================================
class FunctionRegistry:
    '''
    Function ID registry with argument validation.
    
    Features:
        - Register functions with fn_id
        - Validate int args count
        - Validate c_args length (overflow to pool)
        - Variable reference parsing (var:name)
        - Custom delimiter for c_args parsing
    
    Usage:
        registry = FunctionRegistry()
        
        # Register function
        fn_id = registry.register(my_handler, name="my_func", arg_count=2)
        
        # Set variable
        registry.set_var("config", {"key": "value"})
        
        # Validate and pack args
        int_args, c_args, pool_id = registry.prepare_args(
            fn_id, args=(10, 20), c_args="var:config"
        )
    '''
    
    FN_ID_USER_START = 0x8000
    FN_ID_SYSTEM_END = 0x7FFF
    
    def __init__(self, 
                 slot_int_args: int = 2,
                 slot_c_args: int = 64,
                 delimiter: str = ' '):
        '''
        Args:
            slot_int_args: Max int args in slot (default: 2 for TaskSlot128_cargs)
            slot_c_args: Max c_args bytes in slot (default: 64)
            delimiter: c_args parsing delimiter (default: space)
        '''
        self._functions: Dict[int, FunctionEntry] = {}
        self._names: Dict[str, int] = {}  # name -> fn_id
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
        '''Set custom delimiter for c_args parsing.'''
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
        
        # Auto-assign fn_id if not provided
        if fn_id is None:
            fn_id = self._next_fn_id
            self._next_fn_id += 1
        
        # Validation
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
            meta=meta or {},
        )
        
        self._functions[fn_id] = entry
        self._names[name] = fn_id
        
        return fn_id
    
    def get(self, fn_id: int) -> Optional[FunctionEntry]:
        '''Get function entry by fn_id.'''
        return self._functions.get(fn_id)
    
    def get_by_name(self, name: str) -> Optional[FunctionEntry]:
        '''Get function entry by name.'''
        fn_id = self._names.get(name)
        return self._functions.get(fn_id) if fn_id else None
    
    def get_handler(self, fn_id: int) -> Optional[Callable]:
        '''Get handler callable.'''
        entry = self.get(fn_id)
        return entry.handler if entry else None
    
    #----------------------------------------------------------
    # VARIABLES
    #----------------------------------------------------------
    def set_var(self, name: str, value: Any):
        '''Set a named variable for var:name references.'''
        self._pool.set_var(name, value)
    
    def get_var(self, name: str) -> Any:
        '''Get a named variable.'''
        return self._pool.get_var(name)
    
    #Argument VALIDATION LOGIC (SHOULD BE TO BE STATIC!)
    #THAT IS EITHER IT HAPPENS BEFORE CREATING USER FACING INSTANCES
    # OR HAS TO DO ALL VALIDATION BEFORE WORKERS ARE ACTUALLY RUNNING.
    def validate_args(self,
                      fn_id: int,
                      args: tuple,
                      c_args: Union[bytes, str, List[str]] = None) -> Tuple[bool, str]:
        entry = self.get(fn_id)
        
        # Check int args count
        if len(args) > self._slot_int_args:
            return False, f"Too many int args: {len(args)} > {self._slot_int_args}"
        
        # Check function-specific arg count
        if entry and entry.arg_count > 0 and len(args) != entry.arg_count:
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
        
        # Handle variable reference
        if isinstance(c_args, str) and c_args.startswith('var:'):
            var_name = c_args[4:]  # Strip "var:" prefix
            value = self._pool.get_var(var_name)
            if value is None:
                raise ArgValidationError(f"Variable '{var_name}' not found")
            
            # Store in pool and encode pool reference
            pool_id = self._pool.store(value)
            packed = f"pool:{pool_id}".encode('utf-8') + b'\x00'
            return packed, pool_id
        
        # Pack c_args
        if isinstance(c_args, str):
            packed = c_args.encode('utf-8') + b'\x00'
        elif isinstance(c_args, list):
            packed = self._delimiter.encode('utf-8').join(
                s.encode('utf-8') for s in c_args
            ) + b'\x00'
        else:
            packed = c_args
        
        # Check overflow
        if len(packed) > self._slot_c_args:
            # Store in pool
            pool_id = self._pool.store(packed)
            packed = f"pool:{pool_id}".encode('utf-8') + b'\x00'
        
        return packed, pool_id
    
    def resolve_c_args(self, c_args: bytes) -> Any:

        # Check for pool reference
        try:
            text = c_args.rstrip(b'\x00').decode('utf-8')
            if text.startswith('pool:'):
                pool_id = int(text[5:])
                return self._pool.retrieve(pool_id)
        except:
            pass
        
        return c_args
    
    def dispatch(self, fn_id: int, slot, ctx) -> Any:

        handler = self.get_handler(fn_id)
        if handler is None:
            raise FunctionNotFoundError(f"fn_id {fn_id:#06x} not registered")
        return handler(slot, ctx)
    
    def list_functions(self) -> List[Dict]:
        '''List all registered functions.'''
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