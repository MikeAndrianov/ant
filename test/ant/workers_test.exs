defmodule Ant.WorkersTest do
  use Ant.TestCase
  use MnesiaTesting
  use Mimic
  alias Ant.Workers
  alias Ant.WorkerUniquenessChecker

  defmodule TestWorker do
    use Ant.Worker

    def perform(_worker), do: :ok
  end

  defmodule UniqueTestWorker do
    use Ant.Worker, unique: [args: [:email]]

    def perform(_worker), do: :ok
  end

  setup :set_mimic_global
  setup :verify_on_exit!

  setup do
    Mimic.copy(WorkerUniquenessChecker)

    :ok
  end

  describe "list_workers/2" do
    test "returns all workers when no limit is specified" do
      workers = create_test_workers(5)

      assert {:ok, result} = Workers.list_workers(%{})
      assert length(result) == 5
      assert_lists_contain_same(result, workers, equals_by: :id)
    end

    test "returns limited number of workers when limit is specified" do
      create_test_workers(5)

      assert {:ok, result} = Workers.list_workers(%{}, limit: 3)
      assert length(result) == 3
    end

    test "returns filtered and limited workers" do
      create_test_workers(5, status: :failed)
      create_test_workers(3, status: :completed)

      assert {:ok, result} = Workers.list_workers(%{status: :failed}, limit: 2)
      assert length(result) == 2
      assert Enum.all?(result, &(&1.status == :failed))
    end

    test "returns no workers for non-positive limits" do
      create_test_workers(5)

      assert {:ok, []} = Workers.list_workers(%{}, limit: 0)
      assert {:ok, []} = Workers.list_workers(%{}, limit: -1)
    end

    test "ignores non-integer limit values" do
      workers = create_test_workers(5)

      assert {:ok, result} = Workers.list_workers(%{}, limit: "invalid")
      assert length(result) == 5
      assert_lists_contain_same(result, workers, equals_by: :id)
    end
  end

  describe "list_workers/1" do
    test "returns all workers when no limit is specified" do
      workers = create_test_workers(5)

      assert {:ok, result} = Workers.list_workers()
      assert length(result) == 5
      assert_lists_contain_same(result, workers, equals_by: :id)
    end

    test "returns limited number of workers when limit is specified" do
      create_test_workers(5)

      assert {:ok, result} = Workers.list_workers(limit: 3)
      assert length(result) == 3
    end

    test "returns no workers for non-positive limits" do
      create_test_workers(5)

      assert {:ok, []} = Workers.list_workers(limit: 0)
      assert {:ok, []} = Workers.list_workers(limit: -1)
    end

    test "ignores non-integer limit values" do
      workers = create_test_workers(5)

      assert {:ok, result} = Workers.list_workers(limit: "invalid")
      assert length(result) == 5
      assert_lists_contain_same(result, workers, equals_by: :id)
    end
  end

  describe "list_retrying_workers/3" do
    test "returns all retrying workers when no limit is specified" do
      workers = create_test_workers(5, status: :retrying)

      assert {:ok, result} = Workers.list_retrying_workers(%{})
      assert length(result) == 5
      assert_lists_contain_same(result, workers, equals_by: :id)
    end

    test "returns limited number of retrying workers when limit is specified" do
      create_test_workers(5, status: :retrying)

      assert {:ok, result} = Workers.list_retrying_workers(%{}, DateTime.utc_now(), limit: 3)
      assert length(result) == 3
    end

    test "returns the workers scheduled earliest when limit is specified" do
      now = DateTime.utc_now()

      [_w1, w2, _w3, w4] =
        create_scheduled_test_workers(:retrying, now, [-10, -40, -20, -30])

      assert {:ok, result} = Workers.list_retrying_workers(%{}, now, limit: 2)
      assert Enum.map(result, & &1.id) == [w2.id, w4.id]
    end

    test "workers scheduled in the future do not consume the limit" do
      now = DateTime.utc_now()

      [_future1, _future2, due1, due2] =
        create_scheduled_test_workers(:retrying, now, [60, 120, -20, -10])

      assert {:ok, result} = Workers.list_retrying_workers(%{}, now, limit: 2)
      assert Enum.map(result, & &1.id) == [due1.id, due2.id]
    end

    test "returns no workers for non-positive limits" do
      create_test_workers(5, status: :retrying)

      assert {:ok, []} = Workers.list_retrying_workers(%{}, DateTime.utc_now(), limit: 0)
      assert {:ok, []} = Workers.list_retrying_workers(%{}, DateTime.utc_now(), limit: -1)
    end

    test "ignores non-integer limit values" do
      workers = create_test_workers(5, status: :retrying)

      assert {:ok, result} =
               Workers.list_retrying_workers(%{}, DateTime.utc_now(), limit: "invalid")

      assert length(result) == 5
      assert_lists_contain_same(result, workers, equals_by: :id)
    end
  end

  describe "list_scheduled_workers/3" do
    test "returns all scheduled workers when no limit is specified" do
      workers = create_test_workers(5, status: :scheduled)

      assert {:ok, result} = Workers.list_scheduled_workers(%{})
      assert length(result) == 5
      assert_lists_contain_same(result, workers, equals_by: :id)
    end

    test "returns limited number of scheduled workers when limit is specified" do
      create_test_workers(5, status: :scheduled)

      assert {:ok, result} = Workers.list_scheduled_workers(%{}, DateTime.utc_now(), limit: 3)
      assert length(result) == 3
    end

    test "returns the workers scheduled earliest when limit is specified" do
      now = DateTime.utc_now()

      [_w1, w2, _w3, w4] =
        create_scheduled_test_workers(:scheduled, now, [-10, -40, -20, -30])

      assert {:ok, result} = Workers.list_scheduled_workers(%{}, now, limit: 2)
      assert Enum.map(result, & &1.id) == [w2.id, w4.id]
    end

    test "workers scheduled in the future do not consume the limit" do
      now = DateTime.utc_now()

      [_future1, _future2, due1, due2] =
        create_scheduled_test_workers(:scheduled, now, [60, 120, -20, -10])

      assert {:ok, result} = Workers.list_scheduled_workers(%{}, now, limit: 2)
      assert Enum.map(result, & &1.id) == [due1.id, due2.id]
    end

    test "returns no workers for non-positive limits" do
      create_test_workers(5, status: :scheduled)

      assert {:ok, []} = Workers.list_scheduled_workers(%{}, DateTime.utc_now(), limit: 0)
      assert {:ok, []} = Workers.list_scheduled_workers(%{}, DateTime.utc_now(), limit: -1)
    end

    test "ignores non-integer limit values" do
      workers = create_test_workers(5, status: :scheduled)

      assert {:ok, result} =
               Workers.list_scheduled_workers(%{}, DateTime.utc_now(), limit: "invalid")

      assert length(result) == 5
      assert_lists_contain_same(result, workers, equals_by: :id)
    end
  end

  describe "list_enqueued_workers/3" do
    test "returns the enqueued workers in the order they were created" do
      # `filter/3` returns rows in table order, so a limit used to keep an
      # arbitrary subset: an old job could be passed over indefinitely while
      # newer ones ran.
      #
      workers = create_test_workers(5)

      assert {:ok, result} = Workers.list_enqueued_workers(%{}, DateTime.utc_now(), limit: 3)

      assert Enum.map(result, & &1.id) == workers |> Enum.map(& &1.id) |> Enum.take(3)
    end

    test "orders by id rather than by scheduled_at" do
      # An enqueued worker's scheduled_at is the moment it was created, so id
      # order is creation order. Ordering by id is what lets the limit be
      # applied by the query rather than after sorting the whole backlog.
      #
      now = DateTime.utc_now()

      [w1, w2, _w3, _w4] = create_scheduled_test_workers(:enqueued, now, [-10, -40, -20, -30])

      assert {:ok, result} = Workers.list_enqueued_workers(%{}, now, limit: 2)
      assert Enum.map(result, & &1.id) == [w1.id, w2.id]
    end

    test "does not return workers scheduled in the future" do
      now = DateTime.utc_now()

      [_future, due] = create_scheduled_test_workers(:enqueued, now, [60, -10])

      assert {:ok, [worker]} = Workers.list_enqueued_workers(%{}, now)
      assert worker.id == due.id
    end
  end

  describe "list_worker_timestamps/0" do
    test "returns only the id and the timestamps of every worker" do
      workers = create_test_workers(3)

      assert {:ok, result} = Workers.list_worker_timestamps()

      # Compared by id: the rows are partial maps, and sorting those against
      # whole worker structs compares terms of a different shape.
      #
      assert Enum.sort(Enum.map(result, & &1.id)) == Enum.sort(Enum.map(workers, & &1.id))

      assert Enum.all?(result, &(Map.keys(&1) == [:id, :scheduled_at, :updated_at]))
      assert Enum.all?(result, & &1.updated_at)
    end
  end

  describe "create_worker/1" do
    test "creates worker when uniqueness check passes" do
      worker = TestWorker.build(%{email: "test@example.com"})

      expect(WorkerUniquenessChecker, :call, fn ^worker ->
        :ok
      end)

      assert {:ok, created_worker} = Workers.create_worker(worker)
      assert created_worker.id
      assert created_worker.worker_module == TestWorker
      assert created_worker.args == %{email: "test@example.com"}
      assert created_worker.status == :enqueued
    end

    test "returns error when uniqueness check fails" do
      worker = TestWorker.build(%{email: "test@example.com"})

      expect(WorkerUniquenessChecker, :call, fn ^worker ->
        {:error, :already_exists}
      end)

      assert Workers.create_worker(worker) == {:error, :already_exists}
    end

    test "propagates other errors from uniqueness checker" do
      worker = TestWorker.build(%{email: "test@example.com"})

      expect(WorkerUniquenessChecker, :call, fn ^worker ->
        {:error, :database_error}
      end)

      assert Workers.create_worker(worker) == {:error, :database_error}
    end

    test "creates only one worker when the same unique worker is created concurrently" do
      # The uniqueness check and the insert used to run in separate
      # transactions, so every one of these calls found no duplicate and
      # inserted a worker of its own.
      #
      test_pid = self()

      tasks =
        for _ <- 1..10 do
          Task.async(fn ->
            send(test_pid, {:ready, self()})

            receive do
              :go -> UniqueTestWorker.perform_async(%{email: "race@example.com"})
            after
              1_000 -> {:error, :timed_out}
            end
          end)
        end

      # Released together, so the calls really do overlap.
      #
      for _ <- tasks, do: assert_receive({:ready, _pid}, 1_000)
      Enum.each(tasks, &send(&1.pid, :go))

      results = Task.await_many(tasks)

      assert Enum.count(results, &match?({:ok, _worker}, &1)) == 1
      assert Enum.count(results, &(&1 == {:error, :already_exists})) == 9

      assert {:ok, [_worker]} = Workers.list_workers(%{})
    end

    test "calls uniqueness checker with worker containing unique config" do
      worker = UniqueTestWorker.build(%{email: "test@example.com"})

      expect(WorkerUniquenessChecker, :call, fn worker_arg ->
        assert worker_arg.opts[:unique] == [args: [:email]]

        :ok
      end)

      assert {:ok, _created_worker} = Workers.create_worker(worker)
    end
  end

  defp create_test_workers(count, opts \\ []) do
    status = Keyword.get(opts, :status, :enqueued)

    Enum.map(1..count, fn i ->
      {:ok, worker} =
        %{id: i}
        |> TestWorker.build()
        |> Workers.create_worker()

      {:ok, worker} = Workers.update_worker(worker.id, %{status: status})
      worker
    end)
  end

  defp create_scheduled_test_workers(status, date_time, offsets_in_seconds) do
    Enum.map(offsets_in_seconds, fn offset ->
      {:ok, worker} =
        %{offset: offset}
        |> TestWorker.build()
        |> Workers.create_worker()

      {:ok, worker} =
        Workers.update_worker(worker.id, %{
          status: status,
          scheduled_at: DateTime.add(date_time, offset, :second)
        })

      worker
    end)
  end
end
