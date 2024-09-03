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
    use Ant.Worker

    def perform(_worker), do: :error

    def calculate_delay(_worker), do: 0
  end

  defmodule ExceptionWorker do
    use Ant.Worker

    def perform(_worker), do: raise("Custom exception!")

    def calculate_delay(_worker), do: 0
  end

  setup_all do
    # Stop the Ant.WorkersRunner GenServer to prevent it from running during tests
    # because it automatically pick ups workers from the database and runs them,
    # changing their state.
    # It affects the tests from this file that rely on the state of the workers.
    # The GenServer will be restarted after tests.
    #
    :ok = GenServer.stop(Ant.WorkersRunner)

    # Ensure the GenServer is restarted after tests
    on_exit(fn ->
      {:ok, _} = GenServer.start_link(Ant.WorkersRunner, [])
    end)

    :ok
  end

  describe "start_link/1" do
    test "accepts worker struct on start" do
      assert {:ok, _pid} = Worker.start_link(%Worker{})
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
      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} ->
          {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

          assert updated_worker.status == :completed
          assert updated_worker.attempts == 1
          assert updated_worker.errors == []
      end
    end

    test "retries if worker fails" do
      defmodule FailOnceWorker do
        use Ant.Worker

        def perform(%{attempts: 0}), do: :error
        def perform(_worker), do: :ok
      end

      {:ok, worker} =
        %{a: 1}
        |> FailOnceWorker.build()
        |> Ant.Workers.create_worker()

      {:ok, pid} = Worker.start_link(worker)

      assert Worker.perform(pid) == :ok

      ref = Process.monitor(pid)

      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} ->
          {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

          assert updated_worker.status == :completed
          assert updated_worker.attempts == 1
          assert updated_worker.errors == []
      end
    end

    test "stops retrying after reaching max attempts" do
      {:ok, worker} =
        %{a: 1}
        |> FailWorker.build()
        |> Ant.Workers.create_worker()

      {:ok, pid} = Worker.start_link(worker)

      assert Worker.perform(pid) == :ok

      ref = Process.monitor(pid)

      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} ->
          {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

          assert updated_worker.status == :failed
          assert updated_worker.attempts == 3
          assert updated_worker.errors == []
      end
    end

    test "handles exceptions gracefully and updates worker" do
      {:ok, worker} =
        %{a: 1}
        |> ExceptionWorker.build()
        |> Ant.Workers.create_worker()

      {:ok, pid} = Worker.start_link(worker)

      assert Worker.perform(pid) == :ok

      ref = Process.monitor(pid)

      receive do
        {:DOWN, ^ref, :process, ^pid, _reason} ->
          {:ok, updated_worker} = Ant.Repo.get(:ant_workers, worker.id)

          assert updated_worker.status == :failed
          assert updated_worker.attempts == 3

          errors = updated_worker.errors

          assert Enum.all?(errors, & &1.error == "Custom exception!")
          assert Enum.all?(errors, & &1.stack_trace =~ "Ant.WorkerTest.ExceptionWorker.perform/1")
          assert errors |> Enum.map(& &1.attempt) |> Enum.sort() == [1, 2, 3]
      end
    end
  end
end