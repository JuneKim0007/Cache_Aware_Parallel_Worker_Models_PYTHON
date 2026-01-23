import time
from queues import LocalTaskQueue, SharedTaskQueue
from slot import TaskSlot128
import ctypes



##
#   TODO: REFACTORING
#
#

class Worker:
    def __init__(self, shared_queue: SharedTaskQueue, worker_id: int, _worker_internal_id: int, num_task_slot:int = 128):

        self.shared_queue = shared_queue #create a referer
        self.worker_id = worker_id

        #Internally they will be sequential from 0 to num_workers.
        #this is to control the bitmap,
        #TODO: above.
        self._worker_id = _worker_internal_id
        self.LocalTaskQueue = LocalTaskQueue(num_task_slot)

        self.completed_tasks = 0
        self.io_tasks = 0
        self.cpu_tasks = 0

        self.batch_head = 0
        self.batch_tail = 0




    def run(self):
        print(f"[WORKER] [RUN] Worker ID: {self.worker_id} RUNNING ON 'T.S'", flush=True)

        while True:
            try: 
                print("RUNNING TMP")
                time.sleep(5) 
                break
            except Exception as e:
                print(f"[WORKER] [ERR] Worker ID: {self.worker_id} ERR ON: {e}", flush=True)

        print("[WORKER] [TERMINATION] Worker ID: {self.worker_id} RUNNING ON 'T.S'")
        return 


    def process(self, task: TaskSlot128):
        return
    
    def claim_batch(self, batch_size: int):
        """
        Worker claims a batch of tasks atomically.
        Only marks boundaries for a quicker Lock release.
        """

        available_slots = (self.LocalTaskQueue._state._num_slots
                            - self.LocalTaskQueue._state.curr_size)
        available_slots = min(available_slots, batch_size)
        if available_slots <1: return



        #Flip the bit in the bitmap
        bm = self.shared_queue._state._batch_bitmap.value
        bm |= (1 << self._worker_id)
        self.shared_queue._state._batch_bitmap.value = bm

        with self.shared_queue.lock: 
            #if no more task to fetch
            if self.shared_queue._state.head == self.shared_queue._state.tail:
                #Undo the bitmap fliping
                bm ^= (1 << self._worker_id)
                self.shared_queue._state._batch_bitmap.value = bm
                return
        
            self.shared_queue._state._batch_accumulation += batch_size

            # Mark the batch as claimed (update head cursor / metadata)
            mask = self.shared_queue._state.mask
            self.batch_head =self.shared_queue._state.head
            self.batch_tail = (self.batch_head + available_slots) & mask

            #Only update Head for Concurrency.
            self.shared_queue._state.head = self.batch_tail
        #immediately start dequeuing.
        self.dequeue_batch(available_slots)
        return 

    def dequeue_batch(self, count: int):
        #TO DO: move batch_enqueue logic to the worker from the queue.
        #Simply treat queue as nothing more than an array.

        first_slot = self.shared_queue.slots[self.batch_head]
        self.LocalTaskQueue.batch_enqueue(count, first_slot)

        bm = self.shared_queue._state._batch_bitmap.value
        bm ^= (1 << self._worker_id)
        self.shared_queue._state._batch_bitmap.value = bm


        if self.shared_queue._state._batch_bitmap == 0:
            self.shared_queue._state.head += (
                self.shared_queue._state._batch_accumulation & self.shared_queue._state.mask
            )
 #More versatile, it does cpu and I/O work
 # some different scheduling concerns also needed for this 
 #
class IOWorker:
    def run(self):
        print("IOWORKER NOT IMPLEMENTED")
        print("JUST FOR TESTING")
    def batch_log(self):
        pass
    def log(self):
        pass
            
