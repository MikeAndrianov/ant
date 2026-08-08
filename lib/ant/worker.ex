defmodule Ant.Worker do
  @moduledoc """
  A background job, and the process that runs it.

  `use Ant.Worker` in a module that implements `perform/1` to define a job:

      defmodule MyWorker do
        use Ant.Worker, max_attempts: 3, timeout: :timer.seconds(30)

        def perform(%{args: args} = _worker), do: :ok
      end

      MyWorker.perform_async(%{email: "user@example.com"})

  `perform/1` receives the `Ant.Worker` struct and must return `:ok` or
  `{:ok, result}`; anything else counts as a failed attempt, as do exceptions,
  throws, exits and running past the timeout. A failed attempt is retried while
  the worker has attempts left, and marked as `:failed` afterwards.

  Options accepted by `use Ant.Worker` and, per job, by `perform_async/2`:

    * `:queue` - the queue that runs the job. Defaults to the first configured
      queue.
    * `:max_attempts` - how many times the job may run. Defaults to `1`.
    * `:timeout` - how long a single attempt may take, in milliseconds.
      Defaults to `:infinity`.
    * `:unique` - prevents duplicate jobs, see `Ant.WorkerUniquenessChecker`.

  The delay before a retry defaults to ten seconds times the number of attempts
  made, and can be replaced by implementing the optional `calculate_delay/1`
  callback.
  """

  use GenServer
  require Logger

  alias Ant.Workers

  defstruct [
    :id,
    :worker_module,
    :queue_name,
    :args,
    :status,
    :attempts,
    :scheduled_at,
    :updated_at,
    :errors,
    :opts
  ]

  @type t :: %Ant.Worker{
          id: non_neg_integer(),
          worker_module: module(),
          queue_name: atom() | String.t(),
          args: map(),
          status:
            :enqueued | :running | :scheduled | :completed | :failed | :retrying | :cancelled,
          attempts: non_neg_integer(),
          scheduled_at: DateTime.t(),
          updated_at: DateTime.t(),
          errors: [map()],
          opts: keyword()
        }

  @callback perform(worker :: Ant.Worker.t()) :: :ok | {:ok, any()} | {:error, any()}
  @callback calculate_delay(worker :: Ant.Worker.t()) :: non_neg_integer()
  @optional_callbacks calculate_delay: 1

  @default_max_attempts 1
  @default_retry_delay 10_000
  @default_timeout :infinity

  defmacro __using__(opts) do
    max_attempts = Keyword.get(opts, :max_attempts, @default_max_attempts)
    timeout = Keyword.get(opts, :timeout, @default_timeout)
    unique = Keyword.get(opts, :unique, [])

    # Resolved here rather than with `queue_name || default_queue_name()` in the
    # generated code, where a queue given as a literal makes the check dead.
    #
    queue_name =
      case Keyword.get(opts, :queue) do
        nil -> quote(do: Ant.Worker.default_queue_name())
        queue_name -> queue_name
      end

    quote do
      @behaviour Ant.Worker

      @spec perform_async(args :: map(), opts :: keyword()) ::
              {:ok, Ant.Worker.t()} | {:error, any()}
      def perform_async(args, opts \\ []) do
        args
        |> build(opts)
        |> Workers.create_worker()
      end

      def build(args, opts \\ []) do
        opts =
          opts
          |> Keyword.put_new(:max_attempts, unquote(max_attempts))
          |> Keyword.put_new(:timeout, unquote(timeout))
          |> Keyword.put_new(:unique, unquote(unique))

        %Ant.Worker{
          worker_module: __MODULE__,
          args: args,
          queue_name: unquote(queue_name),
          status: :enqueued,
          attempts: 0,
          scheduled_at: DateTime.utc_now(),
          errors: [],
          opts: opts
        }
      end
    end
  end

  # Returns the name of the first queue from the configuration.
  # Queues can be configured as a keyword list (`[default: [concurrency: 5]]`)
  # or as a plain list of names (`["default"]`).
  #
  def default_queue_name do
    case Application.get_env(:ant, :queues) do
      [{queue_name, _config} | _] -> queue_name
      [queue_name | _] -> queue_name
      _ -> "default"
    end
  end

  # Client API
  def start_link(worker) do
    GenServer.start_link(__MODULE__, worker)
  end

  def perform(worker_pid) do
    GenServer.cast(worker_pid, :perform)
  end

  # Server Callbacks
  def init(worker) do
    state = %{
      worker: worker
    }

    {:ok, state}
  end

  def handle_cast(:perform, %{worker: worker} = state) do
    cond do
      not ant_worker?(worker.worker_module) ->
        reject(
          state,
          "#{inspect(worker.worker_module)} does not implement the Ant.Worker behaviour"
        )

      attempts_exhausted?(worker) ->
        # The worker has already exhausted its attempts
        # (e.g. it was recovered in a stuck state after an application restart)
        # and must not run again.
        {:ok, worker} = Workers.update_worker(worker.id, %{status: :failed})

        {:stop, :normal, %{state | worker: worker}}

      true ->
        run(state)
    end
  end

  defp attempts_exhausted?(worker) do
    max_attempts = worker.opts[:max_attempts]

    is_integer(worker.attempts) and is_integer(max_attempts) and worker.attempts >= max_attempts
  end

  # The module to call comes from the database, and Mnesia has no
  # authentication: anything able to write a row - any node that has the cookie
  # - could otherwise get Ant to call any perform/1 in the release.
  #
  defp ant_worker?(module) do
    is_atom(module) and not is_nil(module) and Code.ensure_loaded?(module) and
      function_exported?(module, :perform, 1) and
      Ant.Worker in behaviours(module)
  end

  defp behaviours(module) do
    module.__info__(:attributes)
    |> Keyword.get_values(:behaviour)
    |> List.flatten()
  end

  # A job that is not runnable at all is failed rather than retried: it would
  # fail the same way on every attempt.
  #
  defp reject(state, reason) do
    worker = state.worker

    Logger.error("Ant refused to run worker ##{worker.id}: #{reason}.")

    error = %{
      attempt: worker.attempts,
      error: reason,
      stack_trace: nil,
      attempted_at: DateTime.utc_now()
    }

    {:ok, worker} =
      Workers.update_worker(worker.id, %{status: :failed, errors: [error | worker.errors]})

    {:stop, :normal, %{state | worker: worker}}
  end

  defp run(state) do
    worker = state.worker

    # Status is already set to :running by Queue.run_worker/1
    {:ok, worker} =
      Workers.update_worker(
        worker.id,
        %{
          scheduled_at: nil,
          attempts: worker.attempts + 1
        }
      )

    state = Map.put(state, :worker, worker)

    case perform_job(worker) do
      {:ok, result} -> handle_result(result, state)
      {:exception, exception, stack_trace} -> handle_exception(exception, stack_trace, state)
      {:caught, kind, value, stack_trace} -> handle_caught(kind, value, stack_trace, state)
      :timeout -> handle_timeout(state)
    end
  end

  # The job runs in a task so that it can be given up on: a `perform/1` that
  # hangs would otherwise occupy its slot in the queue forever.
  #
  defp perform_job(worker) do
    task =
      Task.async(fn ->
        try do
          {:ok, worker.worker_module.perform(worker)}
        rescue
          exception ->
            {:exception, exception, __STACKTRACE__}
        catch
          # `rescue` above handles exceptions; without this clause `throw` and
          # `exit` crash the worker process, skipping retries and leaving the
          # job stuck in the :running status.
          kind, value ->
            {:caught, kind, value, __STACKTRACE__}
        end
      end)

    case Task.yield(task, timeout(worker)) || Task.shutdown(task, :brutal_kill) do
      {:ok, result} -> result
      {:exit, reason} -> {:caught, :exit, reason, []}
      nil -> :timeout
    end
  end

  # Workers created before timeouts existed have no :timeout in their opts.
  #
  defp timeout(worker), do: worker.opts[:timeout] || @default_timeout

  defp handle_result(:ok, state), do: complete(state)
  defp handle_result({:ok, _result}, state), do: complete(state)

  # When result is returned, but is not :ok or {:ok, _result}
  # it is considered an error.
  # If the number of attempts is less than the maximum allowed,
  # the worker will be retried.
  # Otherwise, the worker will be stopped.
  #
  defp handle_result(error_result, state) do
    fail(
      %{
        attempt: state.worker.attempts,
        error: "Expected :ok or {:ok, _result}, but got #{inspect(error_result)}",
        stack_trace: nil,
        attempted_at: DateTime.utc_now()
      },
      state
    )
  end

  defp handle_exception(exception, stack_trace, state) do
    fail(
      %{
        attempt: state.worker.attempts,
        error: Map.get(exception, :message, inspect(exception)),
        stack_trace: Exception.format_stacktrace(stack_trace),
        attempted_at: DateTime.utc_now()
      },
      state
    )
  end

  defp handle_caught(kind, value, stack_trace, state) do
    fail(
      %{
        attempt: state.worker.attempts,
        error: Exception.format_banner(kind, value),
        stack_trace: Exception.format_stacktrace(stack_trace),
        attempted_at: DateTime.utc_now()
      },
      state
    )
  end

  defp handle_timeout(state) do
    fail(
      %{
        attempt: state.worker.attempts,
        error: "Worker timed out after #{timeout(state.worker)}ms",
        stack_trace: nil,
        attempted_at: DateTime.utc_now()
      },
      state
    )
  end

  defp complete(state) do
    {:ok, worker} = Workers.update_worker(state.worker.id, %{status: :completed})

    {:stop, :normal, %{state | worker: worker}}
  end

  defp fail(error, state) do
    {:ok, worker} = record_failure(state.worker, error)

    {:stop, :normal, %{state | worker: worker}}
  end

  # Schedules a retry for the worker, or marks it as failed when no attempts are
  # left. Ant.Queue calls this for workers whose process terminated abnormally
  # and therefore could not record the failure themselves.
  #
  @doc false
  @spec record_failure(t(), map()) :: {:ok, t()} | {:error, atom()}
  def record_failure(%__MODULE__{} = worker, error) do
    errors = [error | worker.errors]

    if worker.attempts < worker.opts[:max_attempts] do
      Logger.warning(
        "#{inspect(worker.worker_module)} (worker ##{worker.id}) failed on attempt " <>
          "#{worker.attempts}/#{worker.opts[:max_attempts]}, will retry: #{error.error}"
      )

      scheduled_at = DateTime.add(DateTime.utc_now(), calculate_delay(worker), :millisecond)

      Workers.update_worker(worker.id, %{
        status: :retrying,
        scheduled_at: scheduled_at,
        errors: errors
      })
    else
      Logger.error(
        "#{inspect(worker.worker_module)} (worker ##{worker.id}) failed permanently " <>
          "after attempt #{worker.attempts}/#{worker.opts[:max_attempts]}: #{error.error}"
      )

      Workers.update_worker(worker.id, %{status: :failed, errors: errors})
    end
  end

  def terminate(_reason, _state) do
    :ok
  end

  defp calculate_delay(worker) do
    if function_exported?(worker.worker_module, :calculate_delay, 1) do
      worker.worker_module.calculate_delay(worker)
    else
      @default_retry_delay * worker.attempts
    end
  end
end
