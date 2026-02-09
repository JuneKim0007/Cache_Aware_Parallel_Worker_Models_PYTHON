#!/usr/bin/env python3
import sys
sys.path.insert(0, '/mnt/project')

from api import MpopApi, TaskResult


# User must define a handler logic
# Example 1 Multiplication: x*y
def multiply_handler(slot, ctx):
    '''Multiply two numbers from slot.args'''
    a = slot.args[0]
    b = slot.args[1]
    result = a * b
    
    if ctx.log_func:
        ctx.log_func(f"{a} * {b} = {result}")
    
    return TaskResult(success=True, value=result)

# Example 2: Computing b^exp.
def power_handler(slot, ctx):
    '''Raise to power'''
    base = slot.args[0]
    exp = slot.args[1]
    result = base ** exp
    
    if ctx.log_func:
        ctx.log_func(f"{base} ** {exp} = {result}")
    
    return TaskResult(success=True, value=result)


# Export handlers
HANDLERS = {
    0x8000: multiply_handler,
    0x8001: power_handler,
}


# ============================================================
# STEP 2: Use with MpopApi
# ============================================================

if __name__ == "__main__":
    # Pass this file as handler module
    app = MpopApi(
        workers=4,
        display=True,
        handler_module=__name__,
        debug_delay=0.05,     # Optional!
    )
    
    print("Enqueueing tasks...")
    
    for i in range(10):
        app.enqueue(fn_id=0x8000, args=(i, 10), tsk_id=i)
    
    for i in range(2, 6):
        app.enqueue(fn_id=0x8001, args=(i, 3), tsk_id=i + 100)
    
    print("Running workers...")
    app.run()
    
    print("\n✅ Done! Custom handlers executed in parallel.")
