# Ant

Background job processing library for Elixir focused on simplicity. It uses Mnesia, which comes out of the box with the Elixir ecosystem, as a storage solution for jobs.

## Getting Started

Add `ant` to your list of dependencies in `mix.exs` and run `mix deps.get` to install it:

```elixir
def deps do
  [
    {:ant, "~> 0.1.0"}
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

Workers are stored in the database for 2 weeks. You can change this by setting `ttl` option:

```elixir
config :ant,
  database: [
    ttl: :timer.hours(24 * 7)
  ]
```

For storing data about workers indefinitely, set `ttl` to `:infinity`:

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

1. `Ant.Workers.list_workers()` - returns a list of all workers
It supports filtering by one or multiple attributes:

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

Supported options:
- `limit`: returns up to the specified number of workers.
  ```elixir
  iex(1)> Ant.Workers.list_workers(
  ...(1)>   %{status: :failed},
  ...(1)>   limit: 10
  ...(1)> )
    {:ok,
      [
        %Ant.Worker{...},
        ...
      ]
    }
  ```

2. `Ant.Workers.get_worker(id)` - returns a worker by id
3. `Ant.Workers.delete_worker(worker)` - deletes a worker. It's not recommended to use this function directly.
