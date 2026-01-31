# CACHE_AWARE_Parallel_Worker_Models

This project aims to provide a **Parallelism Model in Python** where workers are distributed and assigned local queues, and workers fetch tasks from a shared queue.

- Queue slots are aligned to multiples of **64 bytes** for cache awareness.
- Each queue slot stores metadata for the worker that indicates which job and which arguments it should run to process queue tasks.
- The system has its own scheduler logic that uses **locks**, unlike other common scheduling models.
- To reduce lock contention, **batching** and **range-reserving** techniques are used.

For more information, refer to:

[A Shared Ring Queue with Batch Ranging and Claiming](https://github.com/JuneKim0007/A-Shared-Ring-Queue-with-Batch-Ranging-Claiming)

---

## Sections (Work in Progress)

- Architecture Overview
- Queue Slot Structure
- Scheduler Design and Locking Strategy
- Worker Task Distribution
- Usage Examples
- Performance Evaluation
