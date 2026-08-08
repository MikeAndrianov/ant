defmodule Ant.Queue do
  use GenServer
  require Logger

  alias Ant.Workers

  @queue_prefix "ant_queue_"
  @check_interval :timer.seconds(5)
  @default_concurrency 5

  # Client API

  def start_link(opts) do
    queue = Keyword.fetch!(opts, :queue)

    GenServer.start_link(__MODULE__, opts, name: get_tuple_identifier(queue))
  end

  def set_concurrency(queue_name, concurrency)
      when (is_binary(queue_name) or is_atom(queue_name)) and is_integer(concurrency) and
             concurrency > 0 do
    GenServer.call(get_tuple_identifier(queue_name), {:set_concurrency, concurrency})
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    queue_name = Keyword.fetch!(opts, :queue)
    config = Keyword.get(opts, :config, [])
    check_interval = Keyword.get(config, :check_interval, @check_interval)
    concurrency = Keyword.get(config, :concurrency, @default_concurrency)

    # Worker processes belong to this queue rather than to a supervisor shared
    # by all of them. A supervisor shuts down when the process that started it
    # exits, so workers never outlive their queue - a restarted queue used to
    # find its own still-running workers in the :running status and start a
    # second process for each of them.
    #
    {:ok, workers_supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one)

    initial_state = %{
      workers_supervisor: workers_supervisor,
      stuck_workers: [],
      # Monitor reference => id of the worker running in the monitored process.
      # Workers are removed when their process goes down, so the size of this map
      # is the number of jobs the queue is currently running.
      #
      processing_workers: %{},
      check_interval: check_interval,
      concurrency: concurrency,
      queue_name: queue_name,
      timer_ref: nil
    }

    {:ok, initial_state, {:continue, :prepare}}
  end

  @impl true
  # After application start make sure to enqueue workers that are stuck in the non-completed state (running and retrying workers).
  # This prevents the situation when the application is restarted and the workers that were not completed
  # are not picked up by the Queue.
  #
  def handle_continue(:prepare, state) do
    {:ok, stuck_workers} = list_stuck_workers(state.queue_name)

    state =
      state
      |> Map.put(:stuck_workers, stuck_workers)
      |> schedule_check(0)

    {:noreply, state}
  end

  @impl true
  def handle_info(:check_workers, state) do
    {:noreply, state |> start_workers() |> schedule_check()}
  end

  # A worker process is monitored while it runs, so its slot is released as soon
  # as it terminates - no matter whether it finished the job, crashed or was
  # killed. This replaces the blocking `dequeue` call the worker used to make,
  # which could time out while the queue was busy scanning the database.
  #
  @impl true
  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    case Map.pop(state.processing_workers, ref) do
      {nil, _processing_workers} ->
        {:noreply, state}

      {worker_id, processing_workers} ->
        if reason != :normal, do: recover_crashed_worker(worker_id, reason)

        # A slot has been released, so check for workers to process immediately.
        #
        {:noreply, schedule_check(%{state | processing_workers: processing_workers}, 0)}
    end
  end

  @impl true
  def handle_call({:set_concurrency, concurrency}, _from, state) do
    # Slots opened by a raised concurrency are filled without waiting
    # for the next scheduled check.
    #
    {:reply, :ok, schedule_check(%{state | concurrency: concurrency}, 0)}
  end

  # Starts as many workers as there are free slots, so that no more than
  # `concurrency` jobs run at the same time. Workers recovered on start
  # (see `handle_continue(:prepare, ...)`) take precedence over the new ones.
  #
  defp start_workers(state) do
    case available_slots(state) do
      slots when slots <= 0 ->
        state

      slots ->
        {stuck_workers, rest} = Enum.split(state.stuck_workers, slots)

        stuck_workers
        |> Enum.reduce(%{state | stuck_workers: rest}, &run_worker/2)
        |> start_pending_workers()
    end
  end

  defp start_pending_workers(state) do
    # Stuck workers started above are already marked as running,
    # so they can not be fetched again here.
    #
    case available_slots(state) do
      slots when slots <= 0 ->
        state

      slots ->
        {:ok, workers} = list_workers_to_process(state.queue_name, limit: slots)

        Enum.reduce(workers, state, &run_worker/2)
    end
  end

  defp available_slots(state), do: state.concurrency - map_size(state.processing_workers)

  # Returns workers that remain in the non-completed state and should be re-run.
  #
  defp list_stuck_workers(queue_name) do
    with {:ok, running_workers} <-
           Workers.list_workers(%{queue_name: queue_name, status: :running}),
         {:ok, retrying_workers} <-
           Workers.list_retrying_workers(%{queue_name: queue_name}, DateTime.utc_now()) do
      {:ok, running_workers ++ retrying_workers}
    end
  end

  # Get workers in priority order: scheduled -> retrying -> enqueued.
  # Each subsequent type gets the limit that is left over from the previous ones;
  # once the limit is exhausted, the remaining types are skipped entirely.
  #
  defp list_workers_to_process(queue_name, opts) do
    limit = Keyword.get(opts, :limit)

    with {:ok, scheduled_workers} <-
           Workers.list_scheduled_workers(
             %{queue_name: queue_name},
             DateTime.utc_now(),
             limit: limit
           ),
         retrying_limit = remaining_limit(limit, scheduled_workers),
         {:ok, retrying_workers} <- fetch_retrying_workers(queue_name, retrying_limit),
         enqueued_limit = remaining_limit(retrying_limit, retrying_workers),
         {:ok, enqueued_workers} <- fetch_enqueued_workers(queue_name, enqueued_limit) do
      {:ok, scheduled_workers ++ retrying_workers ++ enqueued_workers}
    end
  end

  defp remaining_limit(limit, workers), do: max(limit - length(workers), 0)

  defp fetch_retrying_workers(_queue_name, 0), do: {:ok, []}

  defp fetch_retrying_workers(queue_name, limit) do
    Workers.list_retrying_workers(%{queue_name: queue_name}, DateTime.utc_now(), limit: limit)
  end

  defp fetch_enqueued_workers(_queue_name, 0), do: {:ok, []}

  defp fetch_enqueued_workers(queue_name, limit) do
    Workers.list_enqueued_workers(%{queue_name: queue_name}, DateTime.utc_now(), limit: limit)
  end

  defp schedule_check(state), do: schedule_check(state, state.check_interval)

  defp schedule_check(state, check_interval) do
    # Cancels already scheduled check.
    # After a worker process terminates, the next check happens immediately.
    # Without cancelling, the timer would fire multiple times within the check interval.
    #
    if state.timer_ref, do: cancel_check(state.timer_ref)

    timer_ref = Process.send_after(self(), :check_workers, check_interval)

    %{state | timer_ref: timer_ref}
  end

  defp cancel_check(timer_ref) do
    if Process.cancel_timer(timer_ref) == false do
      # The timer had already fired, so the message may be sitting in the mailbox.
      # Dropping it here keeps a burst of terminating workers from scheduling
      # a check per worker.
      #
      receive do
        :check_workers -> :ok
      after
        0 -> :ok
      end
    end

    :ok
  end

  defp run_worker(worker, state) do
    {:ok, running_worker} = Workers.update_worker(worker.id, %{status: :running})

    child_spec = Supervisor.child_spec({Ant.Worker, running_worker}, restart: :temporary)

    case DynamicSupervisor.start_child(state.workers_supervisor, child_spec) do
      {:ok, pid} ->
        # The monitor is set up before the job starts, so a worker that finishes
        # immediately still releases its slot.
        #
        ref = Process.monitor(pid)

        Ant.Worker.perform(pid)

        %{state | processing_workers: Map.put(state.processing_workers, ref, worker.id)}

      error ->
        Logger.error(
          "#{inspect(worker.worker_module)} (worker ##{worker.id}) could not be started: " <>
            "#{inspect(error)}. The worker is returned to the #{worker.status} status."
        )

        {:ok, _worker} = Workers.update_worker(worker.id, %{status: worker.status})

        # A worker recovered as stuck is not in a status the periodic check looks at,
        # so it has to be kept in the state to be retried on the next check.
        #
        if worker.status == :running,
          do: %{state | stuck_workers: [worker | state.stuck_workers]},
          else: state
    end
  end

  # A worker process that terminates abnormally never gets to record the failure
  # itself, which would leave the job in the :running status until the next
  # application restart. Retry (or fail) it here instead.
  #
  defp recover_crashed_worker(worker_id, reason) do
    case Workers.get_worker(worker_id) do
      {:ok, %{status: :running} = worker} ->
        Ant.Worker.record_failure(worker, %{
          attempt: worker.attempts,
          error: "Worker process terminated: #{inspect(reason)}",
          stack_trace: nil,
          attempted_at: DateTime.utc_now()
        })

      # The worker managed to update its status before terminating
      # (or was deleted), so there is nothing to recover.
      #
      _ ->
        :ok
    end
  end

  # Returns tuple identifier for the queue by the given queue name.
  # Is used by Registry to find the queue.
  #
  defp get_tuple_identifier(queue_name),
    do: {:via, Registry, {Ant.QueueRegistry, @queue_prefix <> to_string(queue_name)}}
end
