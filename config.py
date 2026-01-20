import os
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class Config:
    ALIGNMENT_UNIT: int
    SYS_CPU_COUNT: int
    PROC_CPU_COUNT: int
    SYS_MEMORY_TOTAL: int
    PROC_MEMORY_BUDGET: int
    SYS_L1_CACHE_SIZE: int
    SYS_L2_CACHE_SIZE: int
    SYS_L3_CACHE_SIZE: int 
    L1_L2_QUEUE_BUDGET: float
    L3_QUEUE_BUDGET: float


def load_config(**kargs) -> Config:

    def getenv_default(name, typecast, default):

        if name in kargs:
            return typecast(kargs[name])
        
        val = os.getenv(name)
        
        if val is None:
            print(f":::::FAILED TO LOAD {name} ENV VAR; USING DEFAULT ={default}:::::")
            return default
        
        return typecast(val)


    return Config(
        ALIGNMENT_UNIT=getenv_default("ALIGNMENT_UNIT", int, 4),
        SYS_CPU_COUNT=getenv_default("SYS_CPU_COUNT", int, 8),
        PROC_CPU_COUNT=getenv_default("PROC_CPU_COUNT", int, 4),
        SYS_MEMORY_TOTAL=getenv_default("SYS_MEMORY_TOTAL", int, 8_589_934_592),
        PROC_MEMORY_BUDGET=getenv_default("PROC_MEMORY_BUDGET", int, 0.5),
        SYS_L1_CACHE_SIZE=getenv_default("SYS_L1_CACHE_SIZE", int, 32768), 
        SYS_L2_CACHE_SIZE=getenv_default("SYS_L2_CACHE_SIZE", int, 524288),
        SYS_L3_CACHE_SIZE=getenv_default("SYS_L3_CACHE_SIZE", int, 4194304),
        L1_L2_QUEUE_BUDGET=getenv_default("L1_L2_QUEUE_BUDGET", float, 0.8),
        L3_QUEUE_BUDGET=getenv_default("L3_QUEUE_BUDGET", float, 0.7),
    )


config = load_config()
print(f":::::LOADING CONFIG LOGiCS DONE:::::")
