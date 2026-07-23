defmodule Ant.Worker do
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

  defmacro __using__(opts) do
    queue_name =
      Keyword.get(
        opts,
        :queue,
        List.first(Application.get_env(:ant, :queues, ["default"]))
      )

    max_attempts = Keyword.get(opts, :max_attempts, @default_max_attempts)
    unique = Keyword.get(opts, :unique, [])

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
    max_attempts = worker.opts[:max_attempts]

    if is_integer(worker.attempts) and is_integer(max_attempts) and
         worker.attempts >= max_attempts do
      # The worker has already exhausted its attempts
      # (e.g. it was recovered in a stuck state after an application restart)
      # and must not run again.
      {:ok, worker} = Workers.update_worker(worker.id, %{status: :failed})

      stop_worker(worker, state)
    else
      run(state)
    end
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

    try do
      worker
      |> worker.worker_module.perform()
      |> handle_result(state)
    rescue
      exception ->
        handle_exception(exception, __STACKTRACE__, state)
    catch
      # `rescue` above handles exceptions; without this clause `throw` and `exit`
      # crash the worker process, skipping retries and leaving the job
      # stuck in the :running status.
      kind, value ->
        handle_caught(kind, value, __STACKTRACE__, state)
    end
  end

  defp handle_result(:ok, state) do
    {:ok, worker} = Ant.Workers.update_worker(state.worker.id, %{status: :completed})

    stop_worker(worker, state)
  end

  defp handle_result({:ok, _result}, state) do
    {:ok, worker} = Ant.Workers.update_worker(state.worker.id, %{status: :completed})

    stop_worker(worker, state)
  end

  # When result is returned, but is not :ok or {:ok, _result}
  # it is considered an error.
  # If the number of attempts is less than the maximum allowed,
  # the worker will be retried.
  # Otherwise, the worker will be stopped.
  #
  defp handle_result(error_result, state) do
    error = %{
      attempt: state.worker.attempts,
      error: "Expected :ok or {:ok, _result}, but got #{inspect(error_result)}",
      stack_trace: nil,
      attempted_at: DateTime.utc_now()
    }

    record_failure(error, state)
  end

  defp handle_exception(exception, stack_trace, state) do
    error = %{
      attempt: state.worker.attempts,
      error: Map.get(exception, :message, inspect(exception)),
      stack_trace: Exception.format_stacktrace(stack_trace),
      attempted_at: DateTime.utc_now()
    }

    record_failure(error, state)
  end

  defp handle_caught(kind, value, stack_trace, state) do
    error = %{
      attempt: state.worker.attempts,
      error: Exception.format_banner(kind, value),
      stack_trace: Exception.format_stacktrace(stack_trace),
      attempted_at: DateTime.utc_now()
    }

    record_failure(error, state)
  end

  defp record_failure(error, state) do
    worker = state.worker
    errors = [error | worker.errors]

    if worker.attempts < worker.opts[:max_attempts] do
      Logger.warning(
        "#{inspect(worker.worker_module)} (worker ##{worker.id}) failed on attempt " <>
          "#{worker.attempts}/#{worker.opts[:max_attempts]}, will retry: #{error.error}"
      )

      {:ok, worker} = Workers.update_worker(worker.id, %{errors: errors})

      state
      |> Map.put(:worker, worker)
      |> prepare_for_retry()
    else
      Logger.error(
        "#{inspect(worker.worker_module)} (worker ##{worker.id}) failed permanently " <>
          "after attempt #{worker.attempts}/#{worker.opts[:max_attempts]}: #{error.error}"
      )

      {:ok, worker} = Workers.update_worker(worker.id, %{status: :failed, errors: errors})

      stop_worker(worker, state)
    end
  end

  def terminate(_reason, _state) do
    :ok
  end

  defp prepare_for_retry(state) do
    scheduled_at = DateTime.add(DateTime.utc_now(), calculate_delay(state.worker), :millisecond)

    {:ok, worker} =
      Ant.Workers.update_worker(state.worker.id, %{scheduled_at: scheduled_at, status: :retrying})

    stop_worker(worker, state)
  end

  defp stop_worker(worker, state) do
    Ant.Queue.dequeue(worker)

    {:stop, :normal, state}
  end

  defp calculate_delay(worker) do
    if function_exported?(worker.worker_module, :calculate_delay, 1) do
      worker.worker_module.calculate_delay(worker)
    else
      @default_retry_delay * worker.attempts
    end
  end
end
