defmodule Ant.WorkerTest do
  alias Ant.Worker

  use ExUnit.Case
  use MnesiaTesting

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

  defmodule ThrowWorker do
    use Ant.Worker, max_attempts: 2

    def perform(_worker), do: throw(:custom_throw)

    def calculate_delay(_worker), do: 0
  end

  defmodule ExitWorker do
    use Ant.Worker, max_attempts: 2

    def perform(_worker), do: exit(:custom_exit)

    def calculate_delay(_worker), do: 0
  end

  defmodule ExceptionWorkerHandlesExceptionWithoutMessage do
    use Ant.Worker, max_attempts: 3

    def perform(%{args: args}), do: whoops(args)

    def calculate_delay(_worker), do: 0

    defp whoops(%{status: :ok}) do
      nil
    end
  end

  defmodule TimingOutWorker do
    use Ant.Worker, max_attempts: 2, timeout: 20

    def perform(worker) do
      send(worker.args.test_pid, {:performing, self()})

      Process.sleep(:infinity)
    end

    def calculate_delay(_worker), do: 0
  end

  defmodule SlowEnoughWorker do
    use Ant.Worker, timeout: 1_000

    def perform(_worker) do
      Process.sleep(10)

      :ok
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
      assert worker.opts == [unique: [], timeout: :infinity, max_attempts: 1]
    end
  end

  describe "delayed jobs" do
    test "persists a delay without consuming an attempt" do
      before = DateTime.utc_now()
      delay = :timer.hours(24 * 5)

      assert {:ok, worker} = MyTestWorker.perform_async(%{order_id: 1}, schedule_in: delay)
      after_creation = DateTime.utc_now()

      assert worker.status == :scheduled
      assert worker.attempts == 0
      assert worker.errors == []
      assert DateTime.diff(worker.scheduled_at, before, :millisecond) >= delay
      assert DateTime.diff(worker.scheduled_at, after_creation, :millisecond) <= delay
      assert {:ok, ^worker} = Ant.Workers.get_worker(worker.id)
    end

    test "build and create preserve an absolute scheduled time" do
      at = DateTime.add(DateTime.utc_now(), 86_400, :second)
      worker = MyTestWorker.build(%{}, schedule_at: at)

      assert worker.status == :scheduled
      assert {:ok, persisted} = Ant.Workers.create_worker(worker)
      assert persisted.status == :scheduled
      assert persisted.scheduled_at == at
    end

    test "normalizes an offset datetime to UTC" do
      at = %{
        ~U[2099-01-01 10:00:00Z]
        | time_zone: "Etc/GMT-2",
          zone_abbr: "+02",
          utc_offset: 7200
      }

      assert {:ok, worker} = MyTestWorker.perform_async(%{}, schedule_at: at)
      assert worker.scheduled_at == ~U[2099-01-01 08:00:00Z]
    end

    test "zero delay and past timestamps are immediately eligible" do
      assert {:ok, zero} = MyTestWorker.perform_async(%{}, schedule_in: 0)
      assert {:ok, past} = MyTestWorker.perform_async(%{}, schedule_at: ~U[2000-01-01 00:00:00Z])
      assert zero.status == :enqueued
      assert past.status == :enqueued
    end

    test "uses the exact due boundary without consuming the query limit early" do
      now = ~U[2030-01-01 00:00:00.000000Z]
      assert {:ok, :enqueued, ^now} = Worker.schedule([schedule_at: now], now)
      assert {:ok, :enqueued, ^now} = Worker.schedule([schedule_in: 0], now)
      assert {:ok, :scheduled, at} = Worker.schedule([schedule_in: 1], now)
      assert at == DateTime.add(now, 1, :millisecond)

      {:ok, worker} = MyTestWorker.perform_async(%{}, schedule_at: at)
      assert {:ok, []} = Ant.Workers.list_scheduled_workers(%{}, now, limit: 1)
      assert {:ok, [^worker]} = Ant.Workers.list_scheduled_workers(%{}, at, limit: 1)
    end

    for opts <- [
          [schedule_in: -1],
          [schedule_in: 1.5],
          [schedule_in: "5000"],
          [schedule_in: nil],
          [schedule_in: 10 ** 30],
          [schedule_at: nil],
          [schedule_at: "2030-01-01"],
          [schedule_at: ~N[2030-01-01 00:00:00]],
          [schedule_in: 0, schedule_at: ~U[2030-01-01 00:00:00Z]]
        ] do
      test "rejects invalid options #{inspect(opts)} without inserting a job" do
        opts = unquote(Macro.escape(opts))
        assert {:error, {:invalid_schedule, reason}} = MyTestWorker.perform_async(%{}, opts)
        assert is_binary(reason)
        assert_raise ArgumentError, reason, fn -> MyTestWorker.build(%{}, opts) end
        assert {:ok, []} = Ant.Workers.list_workers()
      end
    end
  end

  describe "perform/1" do
    test "runs perform function for the worker and terminates process" do
      {:ok, worker} =
        %{a: 1}
        |> MyTestWorker.build()
        |> Ant.Workers.create_worker()

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

    test "prepares worker for retry when perform throws" do
      {:ok, worker} =
        %{a: 1}
        |> ThrowWorker.build()
        |> Ant.Workers.create_worker()

      {:ok, pid} = Worker.start_link(worker)

      assert Worker.perform(pid) == :ok

      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

      {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

      assert updated_worker.status == :retrying
      assert updated_worker.attempts == 1

      assert [error] = updated_worker.errors
      assert error.error == "** (throw) :custom_throw"
      assert error.stack_trace
    end

    test "prepares worker for retry when perform exits" do
      {:ok, worker} =
        %{a: 1}
        |> ExitWorker.build()
        |> Ant.Workers.create_worker()

      {:ok, pid} = Worker.start_link(worker)

      assert Worker.perform(pid) == :ok

      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

      {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

      assert updated_worker.status == :retrying
      assert updated_worker.attempts == 1

      assert [error] = updated_worker.errors
      assert error.error == "** (exit) :custom_exit"
      assert error.stack_trace
    end

    test "does not run a worker that has already exhausted its attempts" do
      # E.g. a stuck :running worker recovered after an application restart.
      #
      worker_params =
        %{a: 1}
        |> WorkerWithMaxAttempts.build()
        |> Map.merge(%{attempts: 1, status: :running})
        |> Map.from_struct()

      {:ok, worker} = Ant.Repo.insert(:ant_workers, worker_params)

      {:ok, pid} = Worker.start_link(worker)

      assert Worker.perform(pid) == :ok

      ref = Process.monitor(pid)

      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

      {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

      # WorkerWithMaxAttempts.perform/1 raises, so no new error means
      # perform was never called.
      assert updated_worker.status == :failed
      assert updated_worker.attempts == 1
      assert updated_worker.errors == []
    end

    test "handles exceptions without message gracefully" do
      {:ok, worker} =
        %{a: 1}
        |> ExceptionWorkerHandlesExceptionWithoutMessage.build()
        |> Ant.Workers.create_worker()

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

  describe "worker module verification" do
    # Not an Ant.Worker, but shaped like one.
    #
    defmodule ImposterWorker do
      def perform(worker), do: send(worker.args.test_pid, :performed)
    end

    test "refuses to run a module that does not implement the behaviour" do
      worker_params =
        %{test_pid: self()}
        |> MyTestWorker.build()
        |> Map.put(:worker_module, ImposterWorker)
        |> Map.from_struct()

      {:ok, worker} = Ant.Repo.insert(:ant_workers, worker_params)

      {:ok, pid} = Worker.start_link(worker)
      ref = Process.monitor(pid)

      assert Worker.perform(pid) == :ok
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

      refute_received :performed

      {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

      assert updated_worker.status == :failed
      assert updated_worker.attempts == 0

      assert [error] = updated_worker.errors
      assert error.error =~ "does not implement the Ant.Worker behaviour"
    end

    test "refuses to run a module that does not exist" do
      worker_params =
        %{}
        |> MyTestWorker.build()
        |> Map.put(:worker_module, :"Elixir.NoSuchWorkerModule")
        |> Map.from_struct()

      {:ok, worker} = Ant.Repo.insert(:ant_workers, worker_params)

      {:ok, pid} = Worker.start_link(worker)
      ref = Process.monitor(pid)

      assert Worker.perform(pid) == :ok
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

      {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

      assert updated_worker.status == :failed
    end
  end

  describe "timeouts" do
    test "gives up on a job that runs longer than its timeout and retries it" do
      {:ok, worker} =
        %{test_pid: self()}
        |> TimingOutWorker.build()
        |> Ant.Workers.create_worker()

      {:ok, pid} = Worker.start_link(worker)
      ref = Process.monitor(pid)

      assert Worker.perform(pid) == :ok

      assert_receive {:performing, job_pid}, 1_000
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

      # The job is stopped, not left running with nobody waiting for it.
      #
      refute Process.alive?(job_pid)

      {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

      assert updated_worker.status == :retrying
      assert updated_worker.attempts == 1

      assert [error] = updated_worker.errors
      assert error.error == "Worker timed out after 20ms"
      assert error.attempt == 1
    end

    test "fails a job that times out on its last attempt" do
      worker_params =
        %{test_pid: self()}
        |> TimingOutWorker.build()
        |> Map.put(:attempts, 1)
        |> Map.from_struct()

      {:ok, worker} = Ant.Repo.insert(:ant_workers, worker_params)

      {:ok, pid} = Worker.start_link(worker)
      ref = Process.monitor(pid)

      assert Worker.perform(pid) == :ok
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

      {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

      assert updated_worker.status == :failed
      assert updated_worker.attempts == 2
    end

    test "a job can override the timeout of its worker" do
      assert TimingOutWorker.build(%{}).opts[:timeout] == 20
      assert TimingOutWorker.build(%{}, timeout: 5_000).opts[:timeout] == 5_000
    end

    test "lets a job that finishes within its timeout complete" do
      {:ok, worker} =
        %{}
        |> SlowEnoughWorker.build()
        |> Ant.Workers.create_worker()

      {:ok, pid} = Worker.start_link(worker)
      ref = Process.monitor(pid)

      assert Worker.perform(pid) == :ok
      assert_receive {:DOWN, ^ref, :process, ^pid, :normal}, 1_000

      {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

      assert updated_worker.status == :completed
      assert updated_worker.errors == []
    end
  end

  describe "build/2" do
    test "uses provided queue name" do
      worker = TestWorkerWithQueueName.build(%{key: :value})

      assert worker.queue_name == "test_queue"
    end

    test "falls back to the default queue when no queues are configured" do
      worker = MyTestWorker.build(%{key: :value})

      assert worker.queue_name == "default"
    end

    test "uses the first queue from keyword-style configuration" do
      Application.put_env(:ant, :queues, high_priority: [concurrency: 10], low_priority: [])
      on_exit(fn -> Application.delete_env(:ant, :queues) end)

      worker = MyTestWorker.build(%{key: :value})

      assert worker.queue_name == :high_priority
    end

    test "uses the first queue from plain list configuration" do
      Application.put_env(:ant, :queues, ["mailers", "events"])
      on_exit(fn -> Application.delete_env(:ant, :queues) end)

      worker = MyTestWorker.build(%{key: :value})

      assert worker.queue_name == "mailers"
    end

    test "explicit queue name wins over configuration" do
      Application.put_env(:ant, :queues, high_priority: [concurrency: 10])
      on_exit(fn -> Application.delete_env(:ant, :queues) end)

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

    {:ok, pid} = Worker.start_link(worker)

    assert Worker.perform(pid) == :ok

    ref = Process.monitor(pid)

    assert_receive {:DOWN, ^ref, :process, ^pid, _reason}, 1_000

    {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

    assert updated_worker.status == :failed
    assert updated_worker.attempts == 1
  end
end
