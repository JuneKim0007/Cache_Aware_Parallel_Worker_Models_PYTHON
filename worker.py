import time
from queues import LocalTaskQueue, SharedTaskQueue, ConsumerState
from slot import TaskSlot128, ProcTaskFnID
from tasks import dispatch_task, DEFAULT_TASK_DISPATCH
import ctypes


#============================================================
# WORKER
#============================================================
class Worker:

    DEFAULT_BATCH_SIZE = 16
    DEFAULT_NUM_SLOTS = 128
    
    def __init__(self, shared_queue: SharedTaskQueue, worker_id: int, 
                 consumer_id: int, num_task_slot: int | None = None,
                 max_batch_size: int | None = None):
        
        self.shared_queue = shared_queue
        self.worker_id = worker_id
        
        #consumer_id must be unique [0,63] for bitmap coordination
        self._consumer_id = consumer_id
        
        num_task_slot = num_task_slot or self.DEFAULT_NUM_SLOTS
        max_batch_size = max_batch_size or self.DEFAULT_BATCH_SIZE
        
        #Local queue for worker's private task storage
        self.local_queue = LocalTaskQueue(num_task_slot)
        
        #ConsumerState for SharedTaskQueue batch operations
        self.consumer_state = ConsumerState(consumer_id, max_batch_size)
        
        #Stats
        self.completed_tasks = 0
        self.failed_tasks = 0
        
        #Running flag
        self._running = False
        
        #Dispatch map (can be customized per worker)
        self.dispatch_map = DEFAULT_TASK_DISPATCH
        
        print(f"[WORKER][INIT] Worker ID: {self.worker_id} consumer_id: {consumer_id}")

    def run(self):
        print(f"[WORKER][RUN] Worker ID: {self.worker_id} STARTED", flush=True)
        self._running = True
        while self._running:
            try:
                #Attempt to claim a batch from shared queue
                claimed = self._claim_and_fetch_batch()
                
                if not claimed:
                    #No tasks available, brief sleep to avoid spin
                    time.sleep(0.001)
                    continue
                
                #Process all tasks in local buffer
                should_terminate = self._process_local_batch()
                #Finish batch coordination with shared queue
                self.shared_queue.finish_batch(self.consumer_state)
                
                if should_terminate:
                    self._running = False
                    
            except Exception as e:
                print(f"[WORKER][ERR] Worker ID: {self.worker_id} ERR: {e}", flush=True)
                #Reset consumer state on error
                self.consumer_state.batch_size = 0
        print(f"[WORKER][TERMINATION] Worker ID: {self.worker_id} completed={self.completed_tasks} failed={self.failed_tasks}", flush=True)
        return

    def _claim_and_fetch_batch(self) -> bool:

        #SANITY CHECK
        local_available = self.local_queue._state._num_slots - self.local_queue._state.curr_size
        if local_available < 1:
            return False
        shared_available = self.shared_queue.get_actual_occupancy()
        if shared_available < 1:
            return False
        
        #Batching flow
        batch_size = min(local_available, shared_available, self.DEFAULT_BATCH_SIZE)
        
        claimed = self.shared_queue.claim_batch_range(self.consumer_state, batch_size)
        if not claimed:
            return False
        
        #Dequeue batch to consumer's local buffer
        self.shared_queue.dequeue_batch(self.consumer_state)
        
        return True

    def _process_local_batch(self) -> bool:
        batch_size = self.consumer_state.batch_size
        local_buffer = self.consumer_state.local_buffer
        
        if local_buffer is None or batch_size == 0:
            return False
        
        should_terminate = False
        
        for i in range(batch_size):
            slot = local_buffer[i]
            time.sleep(3)
            if slot.fn_id == ProcTaskFnID.TERMINATE:
                should_terminate = True
                self.completed_tasks += 1
                continue
            
            success = dispatch_task(self.worker_id, slot, self.dispatch_map)
            
            if success:
                self.completed_tasks += 1
            else:
                self.failed_tasks += 1
        
        return should_terminate

    def stop(self):
        self._running = False

    def is_running(self) -> bool:
        return self._running


#============================================================
# IO WORKER (placeholder)
#============================================================
class IOWorker:
    '''
    More versatile worker for CPU and I/O work.
    Different scheduling concerns needed.
    '''
    def __init__(self):
        print("[IOWORKER][INIT] NOT IMPLEMENTED", flush=True)
    
    def run(self):
        print("[IOWORKER][RUN] NOT IMPLEMENTED", flush=True)
        print("[IOWORKER] JUST FOR TESTING", flush=True)
    
    def batch_log(self):
        pass
    
    def log(self):
        pass