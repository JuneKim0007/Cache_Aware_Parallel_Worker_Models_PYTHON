#!/usr/bin/env python3
# ============================================================
# MAIN.PY - Usage Examples
# ============================================================

import warnings
warnings.filterwarnings("ignore", message=".*resource_tracker.*")

import time
from api import MpopApi, ProcTaskFnID


def main():
    app = MpopApi(
        workers=4,
        queue_slots=1024,
        display=True,
        auto_terminate=True,
        debug_delay=0.05,
    )
    
    for i in range(100):
        app.enqueue(
            fn_id=ProcTaskFnID.INCREMENT,
            args=(i * 10, 1),
            tsk_id=i + 1,
        )
    
    return app.run()


def dynamic_example():
    app = MpopApi(
        workers=4,
        queue_slots=256,
        display=True,
        auto_terminate=True,
        debug_delay=0.05,
    )
    
    # Task iterator
    tasks = iter(range(100))
    
    def enqueue_next():
        for _ in range(10):
            try:
                i = next(tasks)
                app.enqueue(args=(i * 10, 1), tsk_id=i + 1)
            except StopIteration:
                return False
        time.sleep(0.1)
        return True
    return app.run(enqueue_callback=enqueue_next)


def nodisplay_example():
    
    app = MpopApi(
        workers=4,
        queue_slots=256,
        display=False,  # Suppress TTY
        auto_terminate=True,
    )
    
    for i in range(50):
        app.enqueue(args=(i, 1), tsk_id=i + 1)
    
    return app.run()


def simple_example():
    
    app = MpopApi.simple(workers=2, display=False)
    
    for i in range(20):
        app.enqueue(args=(i, 1), tsk_id=i + 1)
    
    return app.run()


if __name__ == "__main__":
    import sys
    
    cmds = {
        '--simple': simple_example,
        '--dynamic': dynamic_example,
        '--nodisplay': nodisplay_example,
    }
    
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd in cmds:
            exit(cmds[cmd]())
        else:
            print(f"Unknown: {cmd}")
            print(f"Usage: python main.py [{' | '.join(cmds.keys())}]")
            exit(1)
    else:
        exit(main())