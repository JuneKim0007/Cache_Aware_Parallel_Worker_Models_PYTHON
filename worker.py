import time
from queues import LocalTaskQueue, SharedTaskQueue
from slot import TaskSlot128


class Worker:
    def __init__(self, shared_queue: SharedTaskQueue, worker_id: int, num_task_slot:int = 128):
        #it takes config to automatically load slot size and allocate a local task queue
        #accordingly right now not implemented.
        self.shared_queue._config = shared_queue._config
        self.worker_id = worker_id

        # create local queue with fixed num_slots=128
        #Need to 
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
    
    #============================================================
    #Batch Logic
    # It takes the Lock then looks up the cursor (meta data) for the 
    # Shared schedule Queue; 
    # 
    # //Scheduler Logic NOT IMPLEMENTED
    # 
    #============================================================
    def claim_batch(self, batch_size: int):
        """
        Worker claims a batch of tasks atomically.
        Only marks boundaries for a quicker Lock release.
        """
        with self.shared_queue.lock: 
            available = min(self.shared_queue.available_count(), batch_size)
            if available == 0:
                return
                
            # Mark the batch as claimed (update head cursor / metadata)
            self.batch_head =self.self.shared_queue._state.head
            mask = self.shared_queue._state.mask
            self.batch_tail = (self.batch_head + available) & mask
            self.shared_queue._state.head = self.batch_tail
        #immediately start dequeuing.
        self.dequeue_batch(available)
        return 

    def dequeue_batch(self, count: int):
        """
        Move all tasks from the task slots onto the local slots.
        """ 
        #for performance avoid calling self.localtastqueue.start 
        tmp_head = self.LocalTaskQueue.head
        tmp_tail = self.LocalTaskQueue.tail 
                
        for i in range(tmp_head, tmp_tail):
            self.LocalTaskQueue.slots[i &self.LocalTaskQueue._state.mask]= \
                self.shared_queue.slots[i &self.shared_queue._state.mask]
