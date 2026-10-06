defmodule Ant.DatabaseCleanerTest do
  use ExUnit.Case
  use MnesiaTesting
  use Mimic

  alias Ant.Repo
  alias Ant.Workers

  @cutoff ~U[2030-01-01 00:00:00Z]
  @old ~U[2029-01-01 00:00:00Z]

  setup :set_mimic_global
  setup :verify_on_exit!

  setup do
    Mimic.copy(Repo)
    :ok
  end

  test "keeps every active status regardless of age or scheduled time" do
    workers =
      for status <- [:enqueued, :scheduled, :retrying, :running],
          at <- [nil, @old, ~U[2031-01-01 00:00:00Z]] do
        insert_worker(status, @old, at)
      end

    assert :ok = Workers.delete_expired_workers(@cutoff)
    for worker <- workers, do: assert({:ok, ^worker} = Workers.get_worker(worker.id))
  end

  test "deletes only terminal jobs strictly older than the cutoff" do
    expired =
      for status <- [:completed, :failed, :cancelled] do
        insert_worker(status, DateTime.add(@cutoff, -1, :microsecond))
      end

    retained =
      for status <- [:completed, :failed, :cancelled],
          at <- [@cutoff, DateTime.add(@cutoff, 1, :microsecond)] do
        insert_worker(status, at)
      end

    assert :ok = Workers.delete_expired_workers(@cutoff)
    for worker <- expired, do: assert({:error, :not_found} = Workers.get_worker(worker.id))
    for worker <- retained, do: assert({:ok, ^worker} = Workers.get_worker(worker.id))
  end

  test "rechecks a candidate rescheduled after the scan" do
    worker = insert_worker(:failed, @old)

    expect(Repo, :select_columns, fn table, clauses, columns ->
      candidates = Mimic.call_original(Repo, :select_columns, [table, clauses, columns])
      rewrite(%{worker | status: :scheduled, scheduled_at: @old})
      candidates
    end)

    assert :ok = Workers.delete_expired_workers(@cutoff)
    assert {:ok, %{status: :scheduled, updated_at: @old}} = Workers.get_worker(worker.id)
  end

  test "rechecks a candidate whose retention timestamp changed after the scan" do
    worker = insert_worker(:completed, @old)

    expect(Repo, :select_columns, fn table, clauses, columns ->
      candidates = Mimic.call_original(Repo, :select_columns, [table, clauses, columns])
      rewrite(%{worker | updated_at: @cutoff})
      candidates
    end)

    assert :ok = Workers.delete_expired_workers(@cutoff)
    assert {:ok, %{updated_at: @cutoff}} = Workers.get_worker(worker.id)
  end

  test "an already deleted candidate does not stop cleanup" do
    first = insert_worker(:failed, @old)
    second = insert_worker(:completed, @old)

    expect(Repo, :select_columns, fn table, clauses, columns ->
      candidates = Mimic.call_original(Repo, :select_columns, [table, clauses, columns])
      :ok = Workers.delete_worker(first)
      candidates
    end)

    assert :ok = Workers.delete_expired_workers(@cutoff)
    assert {:error, :not_found} = Workers.get_worker(second.id)
  end

  test "cleanup uses the configured TTL and preserves overdue jobs" do
    old = DateTime.add(DateTime.utc_now(), -86_400, :second)
    active = insert_worker(:scheduled, old, old)
    terminal = insert_worker(:completed, old)

    # Exercise the cleanup callback without starting a timer-driven server.
    assert {:noreply, %{ttl: 60_000, interval: 0}} =
             Ant.DatabaseCleaner.handle_info(:cleanup, %{ttl: 60_000, interval: 0})

    assert_receive :cleanup
    assert {:ok, ^active} = Workers.get_worker(active.id)
    assert {:error, :not_found} = Workers.get_worker(terminal.id)
  end

  test "infinite retention disables the cleaner" do
    previous = Application.get_env(:ant, :database)
    Application.put_env(:ant, :database, ttl: :infinity)

    on_exit(fn ->
      if previous,
        do: Application.put_env(:ant, :database, previous),
        else: Application.delete_env(:ant, :database)
    end)

    assert :ignore = Ant.DatabaseCleaner.init([])
  end

  defp insert_worker(status, updated_at, scheduled_at \\ nil) do
    {:ok, worker} = Repo.insert(:ant_workers, %{status: status, scheduled_at: scheduled_at})
    rewrite(%{worker | updated_at: updated_at})
  end

  # The normal update API intentionally refreshes updated_at. Write fixture
  # timestamps directly to exercise retention without waiting for jobs to age.
  defp rewrite(worker) do
    columns = :mnesia.table_info(:ant_workers, :attributes)
    row = List.to_tuple([:ant_workers | Enum.map(columns, &Map.fetch!(worker, &1))])
    {:atomic, :ok} = :mnesia.transaction(fn -> :mnesia.write(row) end)
    worker
  end
end
