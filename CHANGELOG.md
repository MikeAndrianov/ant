# Changelog

## [1.0.0]

Stability release: jobs are no longer lost, duplicated or run past their limits, and queues keep up with large backlogs.

### Added

- Per-job execution timeout: `use Ant.Worker, timeout: ms`, or per job through `perform_async/2`
- Logging of failed attempts, with the worker module, id and attempt count
- `config :ant, start_queues: false`, to enqueue and inspect jobs without queues running them
- Boot migrations for added, removed or reordered columns, indexes, and a changed `persistence_strategy`

### Changed

- Workers are supervised by their queue and tracked with monitors, replacing the blocking `dequeue` call
- `concurrency` is enforced: jobs still running count against a queue's limit
- Jobs are picked up oldest first
- Job ids come from a persisted counter, so they stay unique across restarts
- Ant joins the host application's Mnesia instead of stopping and reconfiguring it
- Only modules implementing the `Ant.Worker` behaviour are run, so a row written straight into Mnesia cannot invoke arbitrary code
- **Breaking:** queues now start in every environment; set `start_queues: false` where they should not
- **Breaking:** `Ant.Workers.list_workers(..., limit: 0)` returns no workers instead of all of them

### Fixed

- Jobs crashing with `** (EXIT) time out` under load, leaving queue slots occupied and processes stuck
- Jobs silently overwritten after a restart, when a new id collided with a persisted one
- A single tick starting unbounded work, and queues running more jobs than their `concurrency`
- Jobs stuck in `:running` when `perform/1` threw, exited, or had its process killed
- A restarted queue running the jobs its own workers were still processing
- Duplicate jobs from concurrent `perform_async/2` calls with the same unique arguments
- `:disc_copies` silently falling back to memory, losing every job on restart
- Full table scans on every queue tick
- Jobs running one extra time after exhausting `max_attempts`

## [0.1.0]

### Added

- Ability to set uniqueness constraints to prevent duplicate job creation

### Changed

- Queue updates worker's status to `running` before spawning a new process to address [race condition issue](https://github.com/MikeAndrianov/ant/pull/5)

## [0.0.3]

### Fixed

- Prevented GenServer crash on retry when exception lacks a `:message` key

## [0.0.2]

### Fixed

- Improved handling of worker completion states
- Optimized the processing of workers in queues.

### Added
- Added better error handling for worker processing states
- Added support for proper worker prioritization in the queue system

## [0.0.1]
### Added
- Initial release
- Basic queue functionality
- Worker processing system
- Mnesia adapter for persistence
