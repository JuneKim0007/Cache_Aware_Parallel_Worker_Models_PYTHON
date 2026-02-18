from typing import List


class ArgParser:
    DEFAULT_TERMINATOR = b'\x00'
    DEFAULT_DELIMITER = b' '
    
    def __init__(self, terminator: bytes = None, delimiter: bytes = None):
        self.terminator = terminator or self.DEFAULT_TERMINATOR
        self.delimiter = delimiter or self.DEFAULT_DELIMITER
    
    def parse(self, c_args: bytes) -> List[bytes]:
        term_idx = c_args.find(self.terminator)
        data = c_args[:term_idx] if term_idx != -1 else c_args
        return data.split(self.delimiter) if data else []
    
    def parse_as_str(self, c_args: bytes, encoding: str = 'utf-8') -> List[str]:
        return [arg.decode(encoding) for arg in self.parse(c_args) if arg]
    
    def pack(self, args: List[bytes]) -> bytes:
        return self.delimiter.join(args) + self.terminator
    
    def pack_str(self, args: List[str], encoding: str = 'utf-8') -> bytes:
        return self.pack([arg.encode(encoding) for arg in args])


def unpack_args(c_args: bytes, encoding: str = 'utf-8') -> List[str]:
    return ArgParser().parse_as_str(c_args, encoding)


def pack_c_args(c_args, delimiter: str = ' ') -> bytes:
    """Pack c_args input to null-terminated bytes.

    Handles str, List[str], and raw bytes. No pool or var: logic —
    that belongs in FunctionRegistry._process_c_args().
    """
    if isinstance(c_args, str):
        return c_args.encode('utf-8') + b'\x00'
    elif isinstance(c_args, list):
        delim = delimiter.encode('utf-8')
        return delim.join(s.encode('utf-8') for s in c_args) + b'\x00'
    return c_args  # already bytes