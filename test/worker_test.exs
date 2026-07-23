defmodule Ant.WorkerTest do
  alias Ant.Worker

  use ExUnit.Case
  use MnesiaTesting
  use Mimic

  setup do
    Mimic.copy(Ant.Queue)

    :ok
  end

  setup :set_mimic_global
  setup :verify_on_exit!

  defmodule MyTestWorker do
    use Ant.Worker

    def perform(_worker), do: :ok

    def calculate_delay(_worker), do: 0
  end

  defmodule FailWorker do
    use Ant.Worker, max_attempts: 3

    def perform(_worker), do: :error

    def calculate_delay(_worker), do: 0
  end

  defmodule ExceptionWorker do
    use Ant.Worker, max_attempts: 3

    def perform(_worker), do: raise("Custom exception!")

    def calculate_delay(_worker), do: 0
  end

  defmodule WorkerWithMaxAttempts do
    use Ant.Worker, max_attempts: 1

    def perform(_worker), do: raise("Custom exception!")

    def calculate_delay(_worker), do: 0
  end

  defmodule FailOnceWorker do
    use Ant.Worker, max_attempts: 3

    def perform(%{attempts: 1}), do: :error
    def perform(_worker), do: :ok

    def calculate_delay(_worker), do: 0
  end

  defmodule ExceptionWorkerHandlesExceptionWithoutMessage do
    use Ant.Worker, max_attempts: 3

    def perform(_worker), do: whoops(%{status: :error})

    def calculate_delay(_worker), do: 0

    defp whoops(%{status: :ok}) do
      nil
    end
  end

  defmodule TestWorkerWithQueueName do
    use Ant.Worker, queue: "test_queue"

    def perform(_worker), do: :ok
  end

  describe "start_link/1" do
    test "accepts worker struct on start" do
      assert {:ok, _pid} = Worker.start_link(%Worker{})
    end
  end

  describe "perform_async/2" do
    test "creates a worker" do
      assert {:ok, worker} =
               MyTestWorker.perform_async(%{email: "test@mail.com", username: "test"})

      assert worker.worker_module == MyTestWorker
      assert worker.args == %{email: "test@mail.com", username: "test"}
      assert worker.status == :enqueued
      assert worker.updated_at
      assert worker.attempts == 0
      assert worker.errors == []
      assert worker.opts == [unique: [], max_attempts: 1]
    end
  end

  describe "perform/1" do
    test "runs perform function for the worker and terminates process" do
      {:ok, worker} =
        %{a: 1}
        |> MyTestWorker.build()
        |> Ant.Workers.create_worker()

      expect(Ant.Queue, :dequeue, fn worker_to_dequeue ->
        assert worker_to_dequeue.id == worker.id

        :ok
      end)

      {:ok, pid} = Worker.start_link(worker)

      assert Worker.perform(pid) == :ok

      ref = Process.monitor(pid)

      # Wait for the process to finish its work
      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000

      {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

      assert updated_worker.status == :completed
      assert updated_worker.attempts == 1
      assert updated_worker.errors == []
    end

    test "prepares worker for retry if it fails" do
      {:ok, worker} =
        %{a: 1}
        |> FailOnceWorker.build()
        |> Ant.Workers.create_worker()

      expect(Ant.Queue, :dequeue, fn worker_to_dequeue ->
        assert worker_to_dequeue.id == worker.id

        :ok
      end)

      {:ok, pid} = Worker.start_link(worker)

      assert Worker.perform(pid) == :ok

      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000

      {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

      assert updated_worker.status == :retrying
      assert updated_worker.attempts == 1
      assert updated_worker.scheduled_at
      assert updated_worker.updated_at

      assert [error] = updated_worker.errors
      assert error.error == "Expected :ok or {:ok, _result}, but got :error"
      assert error.attempt == 1
      refute error.stack_trace
    end

    test "stops retrying if reached max attempts" do
      worker_params =
        %{a: 1}
        |> FailWorker.build()
        |> Map.merge(%{attempts: 2, errors: [%{attempt: 1}, %{attempt: 2}]})
        |> Map.from_struct()

      {:ok, worker} = Ant.Repo.insert(:ant_workers, worker_params)

      expect(Ant.Queue, :dequeue, fn worker_to_dequeue ->
        assert worker_to_dequeue.id == worker.id

        :ok
      end)

      {:ok, pid} = Worker.start_link(worker)

      assert Worker.perform(pid) == :ok

      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000

      {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

      assert length(updated_worker.errors) == 3
      assert updated_worker.status == :failed
      assert updated_worker.attempts == 3
      assert is_nil(updated_worker.scheduled_at)
    end

    test "handles exceptions gracefully and updates worker" do
      worker_params =
        %{a: 1}
        |> ExceptionWorker.build()
        |> Map.merge(%{
          attempts: 2,
          errors: [
            %{
              attempt: 1,
              error: "Custom exception!",
              stack_trace: "Ant.WorkerTest.ExceptionWorker.perform/1"
            },
            %{
              attempt: 2,
              error: "Custom exception!",
              stack_trace: "Ant.WorkerTest.ExceptionWorker.perform/1"
            }
          ]
        })
        |> Map.from_struct()

      {:ok, worker} = Ant.Repo.insert(:ant_workers, worker_params)

      expect(Ant.Queue, :dequeue, fn worker_to_dequeue ->
        assert worker_to_dequeue.id == worker.id

        :ok
      end)

      {:ok, pid} = Worker.start_link(worker)

      assert Worker.perform(pid) == :ok

      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000

      {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

      assert updated_worker.status == :failed
      assert updated_worker.attempts == 3

      errors = updated_worker.errors

      assert Enum.all?(errors, &(&1.error == "Custom exception!"))

      assert Enum.all?(
               errors,
               &(&1.stack_trace =~ "Ant.WorkerTest.ExceptionWorker.perform/1")
             )

      assert errors |> Enum.map(& &1.attempt) |> Enum.sort() == [1, 2, 3]
    end

    test "handles exceptions without message gracefully" do
      {:ok, worker} =
        %{a: 1}
        |> ExceptionWorkerHandlesExceptionWithoutMessage.build()
        |> Ant.Workers.create_worker()

      expect(Ant.Queue, :dequeue, fn worker_to_dequeue ->
        assert worker_to_dequeue.id == worker.id

        :ok
      end)

      {:ok, pid} = Worker.start_link(worker)

      assert Worker.perform(pid) == :ok

      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000

      {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

      assert updated_worker.status == :retrying
      assert updated_worker.attempts == 1
      assert updated_worker.scheduled_at
      assert updated_worker.updated_at

      assert [error] = updated_worker.errors

      assert error.error ==
               "%FunctionClauseError{module: Ant.WorkerTest.ExceptionWorkerHandlesExceptionWithoutMessage, function: :whoops, arity: 1, kind: nil, args: nil, clauses: nil}"

      assert error.attempt == 1
    end
  end

  describe "build/2" do
    test "uses provided queue name" do
      worker = TestWorkerWithQueueName.build(%{key: :value})

      assert worker.queue_name == "test_queue"
    end
  end

  test "allows to set max_attempts" do
    {:ok, worker} =
      %{a: 1}
      |> WorkerWithMaxAttempts.build()
      |> Map.merge(%{
        attempts: 1,
        errors: [
          %{
            # emulating first attempt is already done
            attempt: 1,
            error: "Custom exception!",
            stack_trace: "Ant.WorkerTest.ExceptionWorker.perform/1"
          }
        ]
      })
      |> Ant.Workers.create_worker()

    expect(Ant.Queue, :dequeue, fn worker_to_dequeue ->
      assert worker_to_dequeue.id == worker.id

      :ok
    end)

    {:ok, pid} = Worker.start_link(worker)

    assert Worker.perform(pid) == :ok

    ref = Process.monitor(pid)

    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000

    {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

    assert updated_worker.status == :failed
    assert updated_worker.attempts == 1
  end
end
