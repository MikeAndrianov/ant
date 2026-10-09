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
  Cancels the given job, if it has not started running yet, so that it never
  runs. Takes the job or its id.

  Only the job passed in is affected: if it is enqueued, scheduled or retrying,
  it is moved to the `:cancelled` status, and then retained like completed and
  failed jobs. No job is ever cancelled other than through this function.

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
    Repo.update_where(:ant_workers, id, fn
      %{status: status} when status in @cancellable_statuses -> {:update, %{status: :cancelled}}
      %{status: :cancelled} -> :unchanged
      %{status: status} -> {:error, {:not_cancellable, status}}
    end)
  end

  # A queue lists the workers to start, then marks each one as running, and a
  # worker can change in between: be cancelled, be deleted, or fail and be
  # retried later. Marking it unconditionally ran it anyway - ahead of its retry
  # delay, for a worker recovered on start - or crashed the queue over a deleted
  # one. So it is only marked while its status and due time are still the ones
  # it was listed with.
  #
  @doc false
  @spec mark_running(Ant.Worker.t()) :: {:ok, Ant.Worker.t()} | {:error, any()}
  def mark_running(%{id: id, status: status, scheduled_at: scheduled_at}) do
    Repo.update_where(:ant_workers, id, fn
      %{status: ^status, scheduled_at: ^scheduled_at} -> {:update, %{status: :running}}
      _changed -> {:error, :changed}
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

  # Only the id and the timestamps, without loading every job's args and stack
  # traces.
  #
  @spec list_worker_timestamps() :: {:ok, [map()]}
  def list_worker_timestamps,
    do: {:ok, Repo.select_columns(:ant_workers, %{}, [:id, :updated_at, :scheduled_at])}

  # Removes the finished workers last updated before `cutoff`. Returns `:ok`, or
  # `{:error, failures}` with the `{id, reason}` of every worker that could not
  # be removed: one failure used to end the whole pass, leaving every worker
  # after it for the next one - and with the same worker failing every time,
  # retention never got past it.
  #
  @doc false
  @spec delete_expired_workers(DateTime.t()) :: :ok | {:error, [{integer(), any()}]}
  def delete_expired_workers(cutoff) do
    failures =
      cutoff
      |> list_expired_worker_ids()
      |> Enum.flat_map(fn id ->
        case delete_expired_worker(id, cutoff) do
          :ok -> []
          {:error, reason} -> [{id, reason}]
        end
      end)

    if failures == [], do: :ok, else: {:error, failures}
  end

  # Only the columns needed to tell whether a worker has expired, so a cleanup
  # pass does not have to load every job's args and stack traces.
  #
  defp list_expired_worker_ids(cutoff) do
    for worker <- Repo.select_columns(:ant_workers, %{}, [:id, :status, :updated_at]),
        expired?(worker, cutoff),
        do: worker.id
  end

  # The scan above is only a list of candidates: a worker may have been changed
  # since, or already deleted. So it is checked again, under a lock, before it
  # is removed.
  #
  defp delete_expired_worker(id, cutoff) do
    delete_if_expired = fn worker ->
      if expired?(worker, cutoff), do: :delete, else: :unchanged
    end

    case Repo.update_where(:ant_workers, id, delete_if_expired) do
      {:ok, _unchanged_worker} -> :ok
      {:error, :not_found} -> :ok
      result -> result
    end
  end

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
