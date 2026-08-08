defmodule Ant.Workers do
  @moduledoc """
  Reading and writing the jobs Ant stores.

  `Ant.Worker.perform_async/2` is the usual way to create a job; the functions
  here are for inspecting and managing the ones already created.
  """

  alias Ant.Repo
  alias Ant.WorkerUniquenessChecker

  @spec create_worker(Ant.Worker.t()) :: {:ok, Ant.Worker.t()} | {:error, any()}
  def create_worker(worker) do
    params = %{
      worker_module: worker.worker_module,
      status: :enqueued,
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

  @spec list_enqueued_workers(map(), DateTime.t(), keyword()) :: {:ok, [Ant.Worker.t()]}
  def list_enqueued_workers(clauses, date_time \\ DateTime.utc_now(), opts \\ []),
    do: list_due_workers(clauses, :enqueued, date_time, opts)

  # Only the columns needed to decide whether a worker can be removed, so a
  # cleanup pass does not have to load every job's args and stack traces.
  #
  @spec list_worker_timestamps() :: {:ok, [map()]}
  def list_worker_timestamps,
    do: {:ok, Repo.select_columns(:ant_workers, %{}, [:id, :updated_at, :scheduled_at])}

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
