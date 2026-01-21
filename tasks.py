import hashlib
from slot import TaskFnID, TaskSlot128
from typing import Optional



# Default dispatch map
DEFAULT_TASK_DISPATCH = {
    TaskFnID.HASH: handle_hash,
    TaskFnID.READ_FILE: handle_read_file,
    TaskFnID.STATUS_REPORT: handle_status_report,
    TaskFnID.INCREMENT: handle_increment,
}

