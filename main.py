#!/usr/bin/env python3
"""Example: typed handlers with pythonic enqueue."""

from api import MpopApi, TaskResult

def multiply(a: int, b: int) -> int:
    return a * b

def power(base: int, exp: int) -> int:
    return base ** exp

def process(name: str, items: list, scale: float) -> str:
    return f"{name}: {sum(items) * scale}"

HANDLERS = {
    0x8000: multiply,
    0x8001: power,
    0x8002: process,
}


if __name__ == "__main__":
    app = MpopApi(
        workers=4,
        display=True,
        handler_module=__name__,
        debug_delay=0.05,
    )

    app.register_handlers(handlers_dict=HANDLERS)
    app.validate()

    print("Enqueueing tasks...")

    for i in range(10):
        app.enqueue(multiply, a=i, b=10, tsk_id=i)

    for i in range(2, 6):
        app.enqueue(fn_id=0x8001, args=(i, 3), tsk_id=i + 100)

    app.enqueue(process, name="batch", items=[10, 20, 30], scale=2.5, tsk_id=200)

    print("Running workers...")
    app.run()

    print("\nDone!")