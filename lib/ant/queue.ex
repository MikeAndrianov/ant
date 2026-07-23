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

  def dequeue(worker) do
    GenServer.call(get_tuple_identifier(worker.queue_name), {:dequeue, worker})
  end

  def set_concurrency(queue_name, concurrency)
      when is_binary(queue_name) and is_integer(concurrency) do
    GenServer.call(get_tuple_identifier(queue_name), {:set_concurrency, concurrency})
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    queue_name = Keyword.fetch!(opts, :queue)
    config = Keyword.get(opts, :config, [])
    check_interval = Keyword.get(config, :check_interval, @check_interval)
    concurrency = Keyword.get(config, :concurrency, @default_concurrency)

    initial_state = %{
      stuck_workers: [],
      processing_workers: [],
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
      |> schedule_check()

    {:noreply, state}
  end

  @impl true
  def handle_info(:check_workers, %{stuck_workers: []} = state) do
    {:ok, workers} = list_workers_to_process(state.queue_name, limit: state.concurrency)

    workers
    |> Enum.take(state.concurrency)
    |> Enum.each(&run_worker/1)

    state = schedule_check(state)

    state = %{state | processing_workers: workers ++ state.processing_workers}
    {:noreply, state}
  end

  @impl true
  def handle_info(
        :check_workers,
        %{stuck_workers: stuck_workers, concurrency: concurrency} = state
      )
      when length(stuck_workers) < concurrency do
    {:ok, enqueued_workers} =
      list_workers_to_process(state.queue_name, limit: state.concurrency - length(stuck_workers))

    workers = stuck_workers ++ enqueued_workers
    Enum.each(workers, &run_worker/1)

    state = schedule_check(state)

    state = %{state | processing_workers: workers ++ state.processing_workers, stuck_workers: []}
    {:noreply, state}
  end

  @impl true
  def handle_info(:check_workers, state) do
    stuck_workers = Enum.take(state.stuck_workers, state.concurrency)
    Enum.each(stuck_workers, &run_worker/1)

    state = schedule_check(state)

    state = %{state | stuck_workers: state.stuck_workers -- stuck_workers}
    {:noreply, state}
  end

  @impl true
  def handle_call({:dequeue, worker}, _from, state) do
    processing_workers = Enum.reject(state.processing_workers, &(&1.id == worker.id))

    state =
      if length(processing_workers) < length(state.processing_workers) do
        # Check for any workers to process immediately after dequeuing the current one.
        #
        schedule_check(state, 0)
      else
        state
      end

    {:reply, :ok, %{state | processing_workers: processing_workers}}
  end

  def handle_call({:set_concurrency, concurrency}, _from, state) do
    {:reply, :ok, %{state | concurrency: concurrency}}
  end

  # Helper Functions

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
    Workers.list_workers(%{queue_name: queue_name, status: :enqueued}, limit: limit)
  end

  defp schedule_check(state), do: schedule_check(state, state.check_interval)

  defp schedule_check(state, check_interval) do
    # Cancels already scheduled check.
    # After the worker dequeueing, the next check happens immediately.
    # Without cancelling, the timer would fire multiple times within the check interval.
    #
    current_timer_ref = state.timer_ref
    if current_timer_ref, do: Process.cancel_timer(current_timer_ref)

    timer_ref = Process.send_after(self(), :check_workers, check_interval)

    %{state | timer_ref: timer_ref}
  end

  defp run_worker(worker) do
    {:ok, updated_worker} = Workers.update_worker(worker.id, %{status: :running})

    child_spec = Supervisor.child_spec({Ant.Worker, updated_worker}, restart: :transient)
    {:ok, pid} = DynamicSupervisor.start_child(Ant.WorkersSupervisor, child_spec)

    Ant.Worker.perform(pid)
  end

  # Returns tuple identifier for the queue by the given queue name.
  # Is used by Registry to find the queue.
  #
  defp get_tuple_identifier(queue_name),
    do: {:via, Registry, {Ant.QueueRegistry, @queue_prefix <> to_string(queue_name)}}
end
