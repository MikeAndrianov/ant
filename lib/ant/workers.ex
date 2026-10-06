defmodule Ant.Workers do
  @moduledoc """
  Reading and writing the jobs Ant stores.

  The `perform_async/2` that `Ant.Worker` defines on your worker module is the
  usual way to create a job; the functions here are for inspecting and managing
  the ones already created.
  """

  alias Ant.Repo
  alias Ant.WorkerUniquenessChecker

  @terminal_statuses [:completed, :failed, :cancelled]
  @cancellable_statuses [:enqueued, :scheduled, :retrying]

  @spec create_worker(Ant.Worker.t()) :: {:ok, Ant.Worker.t()} | {:error, any()}
  def create_worker(worker) do
    params = %{
      worker_module: worker.worker_module,
      status: if(worker.status == :scheduled, do: :scheduled, else: :enqueued),
      attempts: 0,
      queue_name: worker.queue_name,
      args: worker.args,
      scheduled_at: worker.scheduled_at,
      errors: [],
      opts: worker.opts
    }

    # The check and the insert have to happen in one transaction. Run
    # separately, two concurrent calls both found no duplicate and both
    # inserted a worker.
    #
    Repo.transaction(fn ->
      with :ok <- lock_duplicates(worker),
           :ok <- WorkerUniquenessChecker.call(worker) do
        Repo.insert(:ant_workers, params)
      end
    end)
  end

  # Creations that could turn out to be duplicates of each other take the same
  # lock, so the second one runs its uniqueness check only after the first has
  # committed. Locking the row itself would not do: the rows are new, and each
  # one gets an id of its own.
  #
  defp lock_duplicates(worker) do
    case WorkerUniquenessChecker.lock_key(worker) do
      nil -> :ok
      key -> Repo.lock(:ant_workers, key)
    end
  end

  @spec update_worker(integer(), map()) :: {:ok, Ant.Worker.t()} | {:error, any()}
  def update_worker(id, params), do: Repo.update(:ant_workers, id, params)

  @doc """
  Cancels a job that has not started running yet, so that it never runs.

  Enqueued, scheduled and retrying jobs move to the `:cancelled` status, and are
  then retained like completed and failed ones. Takes the job or its id.

  Returns `{:ok, worker}` with the cancelled job, also when it was already
  cancelled. A job that is running, completed or failed is left as it is, and
  `{:error, {:not_cancellable, status}}` is returned: a running job can not be
  stopped part of the way through.
  """
  @spec cancel_worker(Ant.Worker.t() | map() | integer()) ::
          {:ok, Ant.Worker.t()} | {:error, any()}
  def cancel_worker(%{id: id}), do: cancel_worker(id)

  # The status is checked and changed under a lock on the job, which is also
  # taken when a queue marks the job as running (see mark_running/1): either the
  # job is cancelled before its queue gets to it, or it is already running.
  #
  def cancel_worker(id) do
    Repo.transaction(fn ->
      :ok = Repo.lock(:ant_workers, id)

      case get_worker(id) do
        {:ok, %{status: status}} when status in @cancellable_statuses ->
          update_worker(id, %{status: :cancelled})

        {:ok, %{status: :cancelled} = worker} ->
          {:ok, worker}

        {:ok, %{status: status}} ->
          {:error, {:not_cancellable, status}}

        error ->
          error
      end
    end)
  end

  # A queue lists the workers to start, then marks each one as running. Marking
  # it unconditionally ran workers cancelled in between, and failed on (and
  # crashed the queue over) the ones deleted in between. So the status is only
  # changed while it is still the one the worker was listed with.
  #
  @doc false
  @spec mark_running(Ant.Worker.t()) :: {:ok, Ant.Worker.t()} | {:error, any()}
  def mark_running(%{id: id, status: status}) do
    Repo.transaction(fn ->
      :ok = Repo.lock(:ant_workers, id)

      case get_worker(id) do
        {:ok, %{status: ^status}} -> update_worker(id, %{status: :running})
        {:ok, %{status: current_status}} -> {:error, {:status_changed, current_status}}
        error -> error
      end
    end)
  end

  @spec list_workers() :: {:ok, [Ant.Worker.t()]}
  @spec list_workers(keyword() | map()) :: {:ok, [Ant.Worker.t()]}
  @spec list_workers(map(), keyword()) :: {:ok, [Ant.Worker.t()]}
  def list_workers(clauses \\ %{}, opts \\ [])

  def list_workers(clauses, opts) when is_map(clauses) do
    limit = Keyword.get(opts, :limit)

    {:ok, Repo.filter(:ant_workers, clauses, limit: limit)}
  end

  def list_workers(opts, []) when is_list(opts) do
    limit = Keyword.get(opts, :limit)

    {:ok, Repo.filter(:ant_workers, %{}, limit: limit)}
  end

  @spec list_retrying_workers(map(), DateTime.t(), keyword()) :: {:ok, [Ant.Worker.t()]}
  def list_retrying_workers(clauses, date_time \\ DateTime.utc_now(), opts \\ []),
    do: list_due_workers(clauses, :retrying, date_time, opts)

  @spec list_scheduled_workers(map(), DateTime.t(), keyword()) :: {:ok, [Ant.Worker.t()]}
  def list_scheduled_workers(clauses, date_time \\ DateTime.utc_now(), opts \\ []),
    do: list_due_workers(clauses, :scheduled, date_time, opts)

  # Unlike the scheduled and retrying ones, enqueued workers are ordered by id
  # rather than by scheduled_at - and ids come from a sequential counter, so
  # that is the order they were created in. The database returns them in that
  # order already, which is what lets the limit be applied by the query instead
  # of after sorting: the enqueued workers are the backlog, and there can be
  # hundreds of thousands of them.
  #
  @spec list_enqueued_workers(map(), DateTime.t(), keyword()) :: {:ok, [Ant.Worker.t()]}
  def list_enqueued_workers(clauses, date_time \\ DateTime.utc_now(), opts \\ []) do
    with {:ok, workers} <-
           list_workers(Map.put(clauses, :status, :enqueued), limit: Keyword.get(opts, :limit)) do
      {:ok, Enum.filter(workers, &due?(&1, date_time))}
    end
  end

  # Only the columns needed to decide whether a worker can be removed, so a
  # cleanup pass does not have to load every job's args and stack traces.
  #
  @spec list_worker_timestamps() :: {:ok, [map()]}
  def list_worker_timestamps,
    do: {:ok, Repo.select_columns(:ant_workers, %{}, [:id, :status, :updated_at, :scheduled_at])}

  @doc false
  @spec delete_expired_workers(DateTime.t()) :: :ok | {:error, any()}
  def delete_expired_workers(cutoff) do
    {:ok, workers} = list_worker_timestamps()

    workers
    |> Enum.filter(&expired?(&1, cutoff))
    |> Enum.reduce_while(:ok, fn worker, :ok ->
      case delete_expired_worker(worker.id, cutoff) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  # The scan above is only a list of candidates. Lock and reread before deleting:
  # a job may have been rescheduled or updated since the scan, or already deleted.
  #
  defp delete_expired_worker(id, cutoff) do
    Repo.transaction(fn ->
      :ok = Repo.lock(:ant_workers, id)
      id |> get_worker() |> delete_if_expired(cutoff)
    end)
  end

  defp delete_if_expired({:ok, worker}, cutoff) do
    if expired?(worker, cutoff), do: delete_worker(worker), else: :ok
  end

  defp delete_if_expired({:error, :not_found}, _cutoff), do: :ok
  defp delete_if_expired(error, _cutoff), do: error

  defp expired?(worker, cutoff) do
    worker.status in @terminal_statuses and DateTime.compare(worker.updated_at, cutoff) == :lt
  end

  @spec get_worker(integer()) :: {:ok, Ant.Worker.t()} | {:error, any()}
  def get_worker(id), do: Repo.get(:ant_workers, id)

  @spec delete_worker(Ant.Worker.t() | map()) :: :ok | {:error, any()}
  def delete_worker(worker), do: Repo.delete(:ant_workers, worker.id)

  # The limit must be applied only after rejecting workers that are not due yet
  # and sorting. Applying it at fetch time returns an arbitrary subset: newer
  # workers could starve older ones, and workers scheduled in the future could
  # consume the limit, hiding workers that are already due.
  #
  defp list_due_workers(clauses, status, date_time, opts) do
    with {:ok, workers} <- list_workers(Map.put(clauses, :status, status)) do
      due_workers =
        workers
        |> Enum.filter(&due?(&1, date_time))
        |> Enum.sort_by(&due_order/1)
        |> maybe_limit(Keyword.get(opts, :limit))

      {:ok, due_workers}
    end
  end

  defp due?(%{scheduled_at: nil}, _date_time), do: true
  defp due?(%{scheduled_at: at}, date_time), do: DateTime.compare(at, date_time) != :gt

  # Oldest first, and by id for workers due at the same time - ids come from a
  # sequential counter, so that is the order the workers were created in.
  #
  defp due_order(%{scheduled_at: nil, id: id}), do: {0, id}

  defp due_order(%{scheduled_at: scheduled_at, id: id}),
    do: {DateTime.to_unix(scheduled_at, :microsecond), id}

  defp maybe_limit(workers, limit) when is_integer(limit) and limit > 0,
    do: Enum.take(workers, limit)

  defp maybe_limit(_workers, limit) when is_integer(limit), do: []
  defp maybe_limit(workers, _limit), do: workers
end
