
from api import MpopApi, ProcTaskFnID


def main():

    #example
    app = MpopApi(
        debug=True,
        debug_delay=0.05,
    )
    app.print_status(config=True, queue=True, workers=True)
    
    for i in range(100):
        app.enqueue(
            fn_id=ProcTaskFnID.INCREMENT,
            args=(i * 10, 1),
            tsk_id=i + 1,
        )
    app.run()
    return


if __name__ == "__main__":
    exit(main())