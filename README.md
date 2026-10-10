# Ant

Background job processing library for Elixir focused on simplicity. It uses Mnesia, which comes out of the box with the Elixir ecosystem, as a storage solution for jobs.

## Getting Started

Add `ant` to your list of dependencies in `mix.exs` and run `mix deps.get` to install it:

```elixir
def deps do
  [
    {:ant, "~> 1.1"}
  ]
end
```

Define a worker module with `perform` function. Argument is a map. Try to use simple argument values: strings, atoms, numbers, etc.

```elixir
defmodule MyWorker do
  use Ant.Worker

  def perform(%{args: %{first: first, second: second}} = _worker) do
    # some logic

    # has to return :ok or {:ok, result}
    # to be considered successful
    :ok
  end
end
```

Create a job to be processed asynchronously:

```elixir
{:ok, worker} = MyWorker.perform_async(%{first: "first", second: 2})
```

Note that the function to create a job is named `perform_async` and not `perform`. It returns a tuple with `:ok` and a worker struct.

### Scheduling jobs

To schedule a job that will be executed in the future:

```elixir
{:ok, worker} = SupplierReminderWorker.perform_async(
  %{order_id: order.id},
  schedule_in: :timer.hours(24 * 5)
)

# Or provide an absolute time:
SupplierReminderWorker.perform_async(
  %{order_id: order.id},
  schedule_at: ~U[2026-10-11 09:00:00Z]
)
```

`schedule_in` is a non-negative integer in **milliseconds**, consistent with Ant's
timeout and retry delay options. `schedule_at` accepts a timezone-aware `DateTime`
and normalizes it to UTC. Use only one of these options per job.

Future jobs have status `:scheduled`.
A zero delay or a timestamp in the past makes the job immediately eligible.

The timestamp is the earliest the job may run. Queue polling (every five seconds
by default) and available capacity can delay execution. Once the job runs, its
normal timeout and retry policy apply.

A delayed job that is no longer needed can be [cancelled](#cancelling-jobs).

## Configuration

You can configure the library to make it more suitable for your use case.

### Queues

By default `ant` uses only one queue `default`, that allows to concurrently process up to 5 jobs. Check interval for new jobs is 5 seconds.

You can define your own queues in configuration files. Example of `config/config.exs`:

```elixir
import Config

config :ant,
  queues: [
    high_priority: [ # queue name
      concurrency: 10, # how many jobs can be processed simultaneously
      check_interval: 1000 # how often to check for new jobs in milliseconds
    ],
    low_priority: [
      concurrency: 1
    ]
  ]
```
Setting queue for a worker:

```elixir
defmodule MyWorker do
  use Ant.Worker, queue: :high_priority

  def perform(args) do
    # ...
  end
end
```
If `queue` is not set explicitly in the worker definition, the first queue from the configuration list is used.

### Retries

By default `ant` doesn't retry failed jobs. If you want to retry a failed job, set `max_attempts` in the worker definition:

```elixir
defmodule MyWorker do
  use Ant.Worker, max_attempts: 3

  def perform(args) do
    # ...
  end
end
```

Each subsequent attempt is delayed by 10 seconds more than the previous one. To change this behavior, implement `calculate_delay` function in the worker:

```elixir
defmodule MyWorker do
  use Ant.Worker, max_attempts: 3

  def perform(args) do
    # ...
  end

  def calculate_delay(worker), do: 10_000 # 10 seconds between each attempt
end
```

### Timeouts

By default a job runs for as long as it needs to. A job that hangs occupies a slot in its queue forever, so it's worth giving jobs that talk to the outside world a `timeout` (in milliseconds):

```elixir
defmodule MyWorker do
  use Ant.Worker, max_attempts: 3, timeout: :timer.seconds(30)

  def perform(args) do
    # ...
  end
end
```

A job that runs longer than that is stopped and treated as a failed attempt: it's retried if it has attempts left, and marked as `:failed` otherwise. The timeout can also be set per job, which overrides the worker definition:

```elixir
MyWorker.perform_async(%{first: "first"}, timeout: :timer.seconds(5))
```

### Database

By default `ant` uses Mnesia with in-memory (`:ram_copies`) persistence strategy. To store jobs on a disk, please use one of the following strategies: `:disc_copies` or `:disc_only_copies`.

Changing the strategy later is applied on the next start: an existing table is converted to the newly configured one.

Jobs are picked up oldest first. With `:disc_only_copies` that ordering isn't guaranteed — the underlying storage has no ordered table — so a queue with a large backlog may run jobs out of order.

For `:disc_copies` and `:disc_only_copies` it's also possible to set custom path to the directory for storing database files using `persistence_dir` option in the configuration.

```elixir
config :ant,
  database: [
    persistence_strategy: :disc_copies,
    persistence_dir:
      "HOME"
      |> System.get_env()
      |> Path.join(["/sandbox", "/ant_sandbox", "/mnesia_db"])
      |> String.to_charlist()
  ]
```

Mnesia reads its directory once, when it starts. `ant` applies `persistence_dir` while Mnesia holds nothing but an empty schema; if your application uses Mnesia itself, the setting is ignored with a warning, since moving the directory would mean taking your own tables down. Configure Mnesia directly in that case:

```elixir
config :mnesia, dir: ~c"/var/lib/my_app/mnesia"
```

Note that disc persistence needs the node to have a name — start the application with `--sname` or `--name`.

Completed, failed and cancelled jobs are retained for 2 weeks after their last
update. You can change this by setting the `ttl` option (in milliseconds):

```elixir
config :ant,
  database: [
    ttl: :timer.hours(24 * 7)
  ]
```

Active jobs (`:enqueued`, `:scheduled`, `:retrying`, and `:running`) are never
removed by retention cleanup, even if they are older than the TTL or overdue.
This keeps delayed jobs safe while they wait for execution. Expired terminal jobs
are removed on the next cleanup pass, which runs hourly (or every `ttl`
milliseconds when the TTL is shorter than an hour).

The TTL must be a positive number of milliseconds, or `:infinity`; anything
else fails when Ant starts. For storing data about workers indefinitely, set `ttl`
to `:infinity`:

```elixir
config :ant,
  database: [
    ttl: :infinity
  ]
```

### Testing

By default queues start with the application and immediately pick up enqueued jobs. In your test environment you may want to enqueue and inspect workers without them being run in the background:

```elixir
# config/test.exs
config :ant, start_queues: false
```

With this setting no queues (and no database cleaner) are started; start `Ant.Queue` manually in tests that need it.

### Job Uniqueness

By default, it's allowed to enqueue multiple jobs with identical arguments. You can prevent insertion of duplicated jobs by configuring uniqueness constraints based on job arguments:

```elixir
defmodule MyUniqueWorker do
  use Ant.Worker, unique: [args: [:email, :user_name]]

  def perform(%{args: %{email: email, user_name: user_name}} = _worker) do
    # Send email logic
    :ok
  end
end
```

With this configuration, attempting to create duplicate jobs will return `{:error, :already_exists}`:

```elixir
args = %{email: "user@example.com", user_name: "john_doe"}

{:ok, worker} = MyUniqueWorker.perform_async(args)
MyUniqueWorker.perform_async(args) # => {:error, :already_exists}
```

#### Configuring Status-Based Uniqueness

By default, uniqueness checking only applies to jobs in active states: `:enqueued`, `:running`, `:scheduled`, `:retrying`. You can customize which statuses are considered for uniqueness checking:

**Check all statuses (including completed/failed jobs):**
```elixir
defmodule MyWorker do
  use Ant.Worker, unique: [args: [:email], statuses: :all]

  def perform(_worker), do: :ok
end
```

**Check only specific statuses:**
```elixir
defmodule MyWorker do
  use Ant.Worker, unique: [args: [:email], statuses: [:enqueued, :running]]

  def perform(_worker), do: :ok
end
```

**Check only a single status:**
```elixir
defmodule MyWorker do
  use Ant.Worker, unique: [args: [:email], statuses: :running]

  def perform(_worker), do: :ok
end
```

**Status configuration examples:**
- `statuses: :all` - Prevents duplicates across all job statuses
- `statuses: [:enqueued, :running]` - Only checks enqueued and running jobs
- `statuses: :completed` - Only prevents duplicates if a completed job exists
- `statuses: []` - Disables uniqueness checking (allows all duplicates)

**Important notes about uniqueness:**

- Default behavior checks active states: `:enqueued`, `:running`, `:scheduled`, `:retrying`
- Uniqueness is scoped to both the worker module and queue
- If any specified unique attribute is missing (`nil`) from the job arguments, uniqueness checking is bypassed
- Only jobs with all specified unique attributes present will be considered for duplicate detection

## Security

### Don't put secrets in job arguments

Job arguments and error stack traces are stored as they are. With `:disc_copies` or `:disc_only_copies` they're written to disk unencrypted, and failed jobs are kept for the retention period (2 weeks by default), so anything in `args` outlives the job itself.

Pass a reference instead of the secret — a user id rather than a password reset token, a record id rather than the card details — and look it up inside `perform/1`.

### Protect Erlang distribution

Mnesia has no authentication of its own: any node that can reach the Erlang distribution port with the right cookie can read and write the jobs table directly. That means it can read every job's arguments, and enqueue jobs of its own.

`ant` will only run a job whose `worker_module` implements the `Ant.Worker` behaviour, so a written-in row can't make it call an arbitrary `perform/1` in your release, but that is a last line of defence, not a substitute for:

- a strong, secret cookie that isn't shared across environments;
- binding distribution to a private interface (`inet_dist_use_interface`) or firewalling the port;
- TLS for distribution when nodes talk across an untrusted network.

## Operations with Workers

Jobs are stored as `%Ant.Worker{}` structs. `Ant.Workers` has functions to look
them up and manage them once they have been created. A job is in one of these
statuses:

- `:enqueued` - waiting for its queue to run it
- `:scheduled` - delayed, waiting for its `scheduled_at` time
- `:running` - being run right now
- `:retrying` - failed an attempt, waiting to be retried at `scheduled_at`
- `:completed` - finished successfully
- `:failed` - failed its last attempt
- `:cancelled` - cancelled before it ran

### Listing jobs

`Ant.Workers.list_workers/2` returns the jobs that match all the given
attributes, or every job when called without any:

```elixir
iex(1)> Ant.Workers.list_workers(%{
...(1)>   queue_name: :default,
...(1)>   status: :failed,
...(1)>   args: %{email: "jane.smith734@@yahoo.com"}
...(1)> })
{:ok,
 [
   %Ant.Worker{
     id: 1150403,
     worker_module: AntSandbox.SendPromotionWorker,
     queue_name: :default,
     args: %{email: "jane.smith734@@yahoo.com"},
     status: :failed,
     attempts: 3,
     scheduled_at: nil,
     updated_at: ~U[2025-01-11 17:31:29.615924Z],
     errors: [
       %{
         error: "Expected :ok or {:ok, _result}, but got {:error, \"Invalid email\"}",
         attempt: 3,
         stack_trace: nil,
         attempted_at: ~U[2025-01-11 17:31:29.615792Z]
       },
       %{
         error: "Expected :ok or {:ok, _result}, but got {:error, \"Invalid email\"}",
         attempt: 2,
         stack_trace: nil,
         attempted_at: ~U[2025-01-11 17:30:56.964108Z]
       },
       %{
         error: "Expected :ok or {:ok, _result}, but got {:error, \"Invalid email\"}",
         attempt: 1,
         stack_trace: nil,
         attempted_at: ~U[2025-01-11 17:30:32.909500Z]
       }
     ],
     opts: [max_attempts: 3]
   }
 ]}
```

`args` matches jobs whose arguments include the given keys and values, so
`%{args: %{email: email}}` also finds jobs that have more arguments than
`email`. Other attributes have to be equal, and an attribute given as `nil`
matches any job. The order of the returned jobs isn't guaranteed.

Pass `limit` to return up to that many jobs:

```elixir
Ant.Workers.list_workers(%{status: :failed}, limit: 10)

# Without filters:
Ant.Workers.list_workers(limit: 10)
```

For example, to see the delayed jobs that are still waiting to run:

```elixir
{:ok, workers} = Ant.Workers.list_workers(%{status: :scheduled})
```

### Getting a job

`Ant.Workers.get_worker/1` returns a job by its id, for example to check how it
went:

```elixir
{:ok, worker} = MyWorker.perform_async(%{first: "first", second: 2})

# Later:
{:ok, %Ant.Worker{status: status, errors: errors}} = Ant.Workers.get_worker(worker.id)
```

It returns `{:error, :not_found}` when there is no job with that id, for example
once a finished job has been removed by [retention cleanup](#database).

### Cancelling jobs

A job that hasn't started running yet can be cancelled, for example once the
supplier has responded and the reminder is no longer needed. Pass the job, or
its id, to `Ant.Workers.cancel_worker/1`:

```elixir
Ant.Workers.cancel_worker(worker)
# => {:ok, %Ant.Worker{status: :cancelled, ...}}

# Or by id, for example one stored with the order:
Ant.Workers.cancel_worker(order.reminder_job_id)
```

Enqueued, scheduled and retrying jobs can be cancelled; their queue will not run
them. Cancelling an already cancelled job returns it unchanged. A running job
isn't stopped part of the way through: for it, as for completed and failed jobs,
`{:error, {:not_cancellable, status}}` is returned and the job is left as it is.
Cancelled jobs are retained like completed and failed ones.

A cancellation can come just too late, when the job has already started. So a
job that may no longer be needed by the time it runs should still check for
that in `perform/1`.

### Deleting jobs

`Ant.Workers.delete_worker/1` removes a job, along with its errors, for good:

```elixir
:ok = Ant.Workers.delete_worker(worker)
```

It's not recommended to use it directly. Deleting a running job doesn't stop
it, and the job's result is lost. To keep a job from running,
[cancel](#cancelling-jobs) it instead; completed, failed and cancelled jobs are
removed by [retention cleanup](#database). Deleting a job that no longer exists
returns `{:error, :not_found}`.
