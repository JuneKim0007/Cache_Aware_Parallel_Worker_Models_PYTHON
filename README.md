# CACHE_AWARE_Parallel_Worker_Models

This API aims to provide a **Parallelism Model in Python** where workers are distributed and assigned local queues, and workers fetch tasks from a shared queue.

- Queue slots are aligned to multiples of **64 bytes** for cache awareness.
- Each queue slot stores metadata for the worker that indicates which job and which arguments it should run to process queue tasks.
- The system has its own scheduler logic that uses **locks**, unlike other common scheduling models.
- To reduce lock contention, **batching** and **range-reserving** techniques are used.


  For more information regarding Scheulder, please refer to: [A Shared Ring Queue with Batch Ranging and Claiming](https://github.com/JuneKim0007/A-Shared-Ring-Queue-with-Batch-Ranging-Claiming)
---

## Sections (Work in Progress)

- Architecture choices sketches
- REFACTORIZATION, clean-up
- More Queue Slot Structure
- Helper functions to faciliate developing
- Worker Task Distribution
- Usage Examples
- Performance Evaluation

## Core Design Choice

- Heavy on Statics (allocation), Light on Dynamics(Run time).
- Pros: many variables and values are preconfigured before creating and allocating workers.
- Cons: Additional layer needed to be implemented.


## Note:
- entryPoint.sh and dockerfile are currently not needed due to the change of design choices.
