defmodule Ant.Workers do
  alias Ant.Repo
  alias Ant.WorkerUniquenessChecker

  @spec create_worker(Ant.Worker.t()) :: {:ok, Ant.Worker.t()} | {:error, atom()}
  def create_worker(worker) do
    with :ok <- WorkerUniquenessChecker.call(worker) do
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

      Repo.insert(:ant_workers, params)
    end
  end

  @spec update_worker(integer(), map()) :: {:ok, Ant.Worker.t()} | {:error, atom()}
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

  @spec get_worker(integer()) :: {:ok, Ant.Worker.t()} | {:error, atom()}
  def get_worker(id), do: Repo.get(:ant_workers, id)

  @spec delete_worker(Ant.Worker.t()) :: :ok
  def delete_worker(worker), do: Repo.delete(:ant_workers, worker.id)

  # The limit must be applied only after rejecting workers that are not due yet
  # and sorting by scheduled_at. Applying it at fetch time returns an arbitrary
  # subset: newer workers could starve older ones, and workers scheduled in the
  # future could consume the limit, hiding workers that are already due.
  #
  defp list_due_workers(clauses, status, date_time, opts) do
    with {:ok, workers} <- list_workers(Map.put(clauses, :status, status)) do
      due_workers =
        workers
        |> Enum.reject(&(DateTime.compare(&1.scheduled_at, date_time) == :gt))
        |> Enum.sort_by(& &1.scheduled_at, DateTime)
        |> maybe_limit(Keyword.get(opts, :limit))

      {:ok, due_workers}
    end
  end

  defp maybe_limit(workers, limit) when is_integer(limit) and limit > 0,
    do: Enum.take(workers, limit)

  defp maybe_limit(_workers, limit) when is_integer(limit), do: []
  defp maybe_limit(workers, _limit), do: workers
end
