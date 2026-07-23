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

    test "calls uniqueness checker with worker containing unique config" do
      defmodule UniqueTestWorker do
        use Ant.Worker, unique: [args: [:email]]

        def perform(_worker), do: :ok
      end

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
end
