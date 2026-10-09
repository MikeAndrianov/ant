defmodule Ant.Queue do
  @moduledoc """
  Runs the jobs of one queue, up to `concurrency` of them at a time.

  A queue is started for every entry in the `:queues` configuration and checks
  the database every `check_interval` milliseconds for work to pick up, in this
  order: workers left behind by a previous run, then scheduled, retrying and
  enqueued workers that are due, oldest first.

  Each job runs in its own process, supervised by - and monitored by - the
  queue, which is how a slot is released: when a worker process terminates, for
  any reason, the queue frees its slot and looks for more work right away. A
  worker that terminates without recording its own result is retried or failed
  by the queue.

  Scheduled workers are the exception: finding the due ones means reading every
  one of them, so they are looked up once per `check_interval`, however busy the
  queue is - unless the previous lookup took every free slot.
  """

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
      # Monotonic time (in milliseconds) of the last lookup of scheduled workers,
      # or nil when the next check has to look them up.
      #
      scheduled_lookup_at: nil,
      # Whether the next check should come right away, because a slot was left
      # free during this one.
      #
      check_again: false,
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
        {:ok, workers, state} = list_workers_to_process(state, slots)

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
  defp list_workers_to_process(state, limit) do
    queue_name = state.queue_name

    with {:ok, scheduled_workers, state} <- fetch_scheduled_workers(state, limit),
         retrying_limit = remaining_limit(limit, scheduled_workers),
         {:ok, retrying_workers} <- fetch_retrying_workers(queue_name, retrying_limit),
         enqueued_limit = remaining_limit(retrying_limit, retrying_workers),
         {:ok, enqueued_workers} <- fetch_enqueued_workers(queue_name, enqueued_limit) do
      {:ok, scheduled_workers ++ retrying_workers ++ enqueued_workers, state}
    end
  end

  defp remaining_limit(limit, workers), do: max(limit - length(workers), 0)

  # Finding the scheduled workers that are due means reading every scheduled
  # worker, and delayed jobs pile up - think of a reminder for every order of
  # the past few days. A busy queue checks for work every time one of its jobs
  # finishes, and reading all of them on each of those checks cut its throughput
  # by more than ten times with 10,000 delayed jobs. So they are looked up once
  # per check interval, as often as an idle queue checks - unless the last
  # lookup filled every free slot, and more of them may be due.
  #
  defp fetch_scheduled_workers(state, limit) do
    now = monotonic_time()

    if scheduled_lookup_due?(state, now) do
      lookup_scheduled_workers(state, limit, now)
    else
      {:ok, [], state}
    end
  end

  defp scheduled_lookup_due?(%{scheduled_lookup_at: nil}, _now), do: true

  defp scheduled_lookup_due?(state, now),
    do: now - state.scheduled_lookup_at >= state.check_interval

  # A lookup that filled every free slot may have left due workers behind, so
  # the next check looks again rather than waiting for the interval.
  #
  defp lookup_scheduled_workers(state, limit, now) do
    with {:ok, workers} <-
           Workers.list_scheduled_workers(
             %{queue_name: state.queue_name},
             DateTime.utc_now(),
             limit: limit
           ) do
      lookup_at = if length(workers) < limit, do: now

      {:ok, workers, %{state | scheduled_lookup_at: lookup_at}}
    end
  end

  defp fetch_retrying_workers(_queue_name, 0), do: {:ok, []}

  defp fetch_retrying_workers(queue_name, limit) do
    Workers.list_retrying_workers(%{queue_name: queue_name}, DateTime.utc_now(), limit: limit)
  end

  defp fetch_enqueued_workers(_queue_name, 0), do: {:ok, []}

  defp fetch_enqueued_workers(queue_name, limit) do
    Workers.list_enqueued_workers(%{queue_name: queue_name}, DateTime.utc_now(), limit: limit)
  end

  defp schedule_check(%{check_again: true} = state),
    do: schedule_check(%{state | check_again: false}, 0)

  defp schedule_check(state), do: schedule_check(state, next_check_in(state))

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

  # Checks triggered by finished jobs skip the scheduled workers, and they also
  # push back the next periodic check. So that check is set for when the next
  # lookup of scheduled workers is due, which keeps their wait within the check
  # interval.
  #
  defp next_check_in(%{scheduled_lookup_at: nil} = state), do: state.check_interval

  defp next_check_in(state) do
    case state.scheduled_lookup_at + state.check_interval - monotonic_time() do
      until_lookup when until_lookup > 0 -> until_lookup
      _overdue -> state.check_interval
    end
  end

  defp monotonic_time, do: System.monotonic_time(:millisecond)

  defp run_worker(worker, state) do
    case Workers.mark_running(worker) do
      {:ok, running_worker} ->
        start_worker(worker, running_worker, state)

      # Changed (e.g. cancelled) or deleted since it was listed, so it must not
      # run. Its share of the limit has gone unused, though: the next check
      # comes right away to fill the slot, rather than a check interval later.
      #
      {:error, reason} when reason in [:changed, :not_found] ->
        %{state | check_again: true}

      {:error, reason} ->
        Logger.error(
          "#{inspect(worker.worker_module)} (worker ##{worker.id}) could not be marked as " <>
            "running: #{inspect(reason)}. It is left in the #{worker.status} status."
        )

        keep_if_recovered(worker, state)
    end
  end

  defp start_worker(worker, running_worker, state) do
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

        return_worker(worker, state)
    end
  end

  # A worker that could not be started goes back to the status it was listed
  # with, to be picked up again - unless it has been deleted in the meantime.
  #
  defp return_worker(worker, state) do
    case Workers.update_worker(worker.id, %{status: worker.status}) do
      {:ok, _worker} -> keep_if_recovered(worker, state)
      {:error, :not_found} -> state
    end
  end

  # A worker recovered as stuck is not in a status the periodic check looks at,
  # so it has to be kept in the state to be retried on the next check.
  #
  defp keep_if_recovered(%{status: :running} = worker, state),
    do: %{state | stuck_workers: [worker | state.stuck_workers]}

  defp keep_if_recovered(_worker, state), do: state

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
