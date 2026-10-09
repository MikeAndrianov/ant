defmodule Ant.QueueTest do
  use ExUnit.Case
  use MnesiaTesting
  use Mimic

  alias Ant.Queue
  alias Ant.Repo

  # Workers report to the test process and block until the test tells them how
  # to finish. This gives every test full control over how long a job "runs",
  # without sleeping, and lets it observe the queue with real processes.
  #
  defmodule ControlledWorker do
    use Ant.Worker, max_attempts: 3

    def perform(worker) do
      send(worker.args.test_pid, {:started, worker.id, self()})

      receive do
        {:finish, result} -> result
      after
        5_000 -> {:error, :timed_out}
      end
    end

    # Long enough to keep a retried job from being picked up again
    # while the test is still asserting on it.
    #
    def calculate_delay(_worker), do: :timer.minutes(1)
  end

  defmodule SingleAttemptWorker do
    use Ant.Worker, max_attempts: 1

    defdelegate perform(worker), to: ControlledWorker
  end

  describe "processing workers" do
    test "runs an enqueued worker and releases its slot when it finishes" do
      queue_name = "releases_slot"
      [first, second] = for _ <- 1..2, do: create_worker(queue_name).id

      queue = start_queue(queue_name, concurrency: 1)

      assert_receive {:started, ^first, pid}, 1_000
      assert worker_status(first) == :running
      assert processing_worker_ids(queue) == [first]

      finish(pid, :ok)

      # The freed slot is taken by the next worker without waiting
      # for the next scheduled check.
      #
      assert_receive {:started, ^second, _pid}, 1_000

      assert wait_until(fn -> worker_status(first) == :completed end)
      assert processing_worker_ids(queue) == [second]
    end

    test "runs scheduled, retrying and enqueued workers that are due" do
      queue_name = "due_workers"
      past = DateTime.add(DateTime.utc_now(), -60, :second)

      scheduled = create_worker(queue_name, %{status: :scheduled, scheduled_at: past})
      retrying = create_worker(queue_name, %{status: :retrying, scheduled_at: past, attempts: 1})
      enqueued = create_worker(queue_name)

      start_queue(queue_name, concurrency: 3)

      assert Enum.sort(started_ids(3)) == Enum.sort([scheduled.id, retrying.id, enqueued.id])
    end

    test "does not run workers that are not due yet" do
      queue_name = "not_due_workers"
      future = DateTime.add(DateTime.utc_now(), 60, :second)

      create_worker(queue_name, %{status: :scheduled, scheduled_at: future})
      create_worker(queue_name, %{status: :retrying, scheduled_at: future, attempts: 1})

      start_queue(queue_name, concurrency: 5)

      refute_receive {:started, _id, _pid}, 300
    end
  end

  defmodule DelayedWorker do
    use Ant.Worker, queue: "delayed_public_api", max_attempts: 2

    defdelegate perform(worker), to: ControlledWorker
    def calculate_delay(_worker), do: 0
  end

  describe "delayed jobs through the public API" do
    test "waits across queue restarts, then executes an overdue job and retries normally" do
      {:ok, worker} = DelayedWorker.perform_async(%{test_pid: self()}, schedule_in: 86_400_000)
      queue = start_queue("delayed_public_api", concurrency: 1)
      refute_receive {:started, _, _}, 100
      assert processing_worker_ids(queue) == []
      stop_queue(queue)

      restarted = start_queue("delayed_public_api", concurrency: 1)
      refute_receive {:started, _, _}, 100

      assert {:ok, %{status: :scheduled, attempts: 0, errors: []}} =
               Repo.get(:ant_workers, worker.id)

      stop_queue(restarted)

      # Advance the persisted deadline instead of waiting a day in this test.
      {:ok, _} = Ant.Workers.update_worker(worker.id, %{scheduled_at: ~U[2000-01-01 00:00:00Z]})
      start_queue("delayed_public_api", concurrency: 1)
      id = worker.id
      assert_receive {:started, ^id, first_pid}, 1_000
      assert {:ok, %{attempts: 1, errors: [], scheduled_at: nil}} = Repo.get(:ant_workers, id)
      finish(first_pid, :error)

      assert_receive {:started, ^id, retry_pid}, 1_000
      assert {:ok, %{attempts: 2, errors: [_]}} = Repo.get(:ant_workers, id)
      finish(retry_pid, :ok)
      assert wait_until(fn -> worker_status(id) == :completed end)
    end

    test "never runs a delayed job cancelled before it is due" do
      {:ok, worker} = DelayedWorker.perform_async(%{test_pid: self()}, schedule_in: 86_400_000)
      assert {:ok, %{status: :cancelled}} = Ant.Workers.cancel_worker(worker)

      # Its time comes, but it stays cancelled.
      {:ok, _} = Ant.Workers.update_worker(worker.id, %{scheduled_at: ~U[2000-01-01 00:00:00Z]})
      start_queue("delayed_public_api", concurrency: 1)

      refute_receive {:started, _id, _pid}, 300
      assert {:ok, %{status: :cancelled, attempts: 0}} = Repo.get(:ant_workers, worker.id)
    end
  end

  describe "workers changed after they were listed" do
    setup do
      Mimic.copy(Ant.Workers)
      set_mimic_global(%{})

      :ok
    end

    test "does not run a worker cancelled before the queue marks it as running" do
      queue_name = "cancelled_after_listing"
      past = DateTime.add(DateTime.utc_now(), -60, :second)
      scheduled = create_worker(queue_name, %{status: :scheduled, scheduled_at: past})
      enqueued = create_worker(queue_name)

      after_listing_scheduled(fn ->
        {:ok, %{status: :cancelled}} = Ant.Workers.cancel_worker(scheduled)
      end)

      start_queue(queue_name, concurrency: 2)

      enqueued_id = enqueued.id
      assert_receive {:started, ^enqueued_id, _pid}, 1_000
      refute_receive {:started, _id, _pid}, 300
      assert worker_status(scheduled.id) == :cancelled
    end

    test "skips a worker deleted before the queue marks it as running" do
      queue_name = "deleted_after_listing"
      past = DateTime.add(DateTime.utc_now(), -60, :second)
      scheduled = create_worker(queue_name, %{status: :scheduled, scheduled_at: past})
      enqueued = create_worker(queue_name)

      after_listing_scheduled(fn -> :ok = Ant.Workers.delete_worker(scheduled) end)

      queue = start_queue(queue_name, concurrency: 2)

      enqueued_id = enqueued.id
      assert_receive {:started, ^enqueued_id, _pid}, 1_000
      assert Process.alive?(queue)
      assert processing_worker_ids(queue) == [enqueued_id]
    end

    test "fills a slot left free by a skipped worker without waiting for the next check" do
      # The cancelled worker took the only slot's share of the limit, so no
      # enqueued worker was listed alongside it.
      #
      queue_name = "slot_left_by_skipped_worker"
      past = DateTime.add(DateTime.utc_now(), -60, :second)
      scheduled = create_worker(queue_name, %{status: :scheduled, scheduled_at: past})
      enqueued = create_worker(queue_name)

      after_listing_scheduled(fn ->
        {:ok, %{status: :cancelled}} = Ant.Workers.cancel_worker(scheduled)
      end)

      start_queue(queue_name, concurrency: 1)

      enqueued_id = enqueued.id
      assert_receive {:started, ^enqueued_id, _pid}, 1_000
    end

    test "does not start a retrying worker again when it changed since it was recovered" do
      # Workers recovered on start are kept in the queue's state until a slot is
      # free. The first one is cancelled, so its slot goes to the second, which
      # is due as well: it runs, fails, and is retrying again - a minute later.
      # Its recovered copy must not start it ahead of that.
      #
      queue_name = "changed_since_recovered"
      past = DateTime.add(DateTime.utc_now(), -60, :second)
      attrs = %{status: :retrying, scheduled_at: past, attempts: 1}
      [first, second] = for _ <- 1..2, do: create_worker(queue_name, attrs)

      expect(Ant.Workers, :list_retrying_workers, fn clauses, date_time ->
        result = Mimic.call_original(Ant.Workers, :list_retrying_workers, [clauses, date_time])
        {:ok, %{status: :cancelled}} = Ant.Workers.cancel_worker(first)
        result
      end)

      start_queue(queue_name, concurrency: 1)

      second_id = second.id
      assert_receive {:started, ^second_id, pid}, 1_000
      finish(pid, :error)

      refute_receive {:started, _id, _pid}, 300
      assert worker_status(second_id) == :retrying
    end
  end

  describe "looking up scheduled workers" do
    setup do
      Mimic.copy(Ant.Workers)
      set_mimic_global(%{})

      :ok
    end

    test "a busy queue looks them up once per check interval, not after every job" do
      # Finding the due ones means reading every scheduled worker, and there can
      # be hundreds of thousands of delayed jobs.
      #
      queue_name = "scheduled_lookups_when_busy"
      ids = for _ <- 1..3, do: create_worker(queue_name).id
      test_pid = self()

      stub(Ant.Workers, :list_scheduled_workers, fn clauses, date_time, opts ->
        send(test_pid, :scheduled_lookup)
        Mimic.call_original(Ant.Workers, :list_scheduled_workers, [clauses, date_time, opts])
      end)

      start_queue(queue_name, concurrency: 1)

      for id <- ids do
        assert_receive {:started, ^id, pid}, 1_000
        finish(pid, :ok)
      end

      assert_received :scheduled_lookup
      refute_receive :scheduled_lookup, 300
    end

    test "a busy queue runs a scheduled worker soon after it is due, not after its backlog" do
      queue_name = "scheduled_due_while_busy"
      check_interval = 100
      for _ <- 1..60, do: create_worker(queue_name)

      # Read before the due time, so that the measured wait is never shorter
      # than the real one.
      #
      due_at_monotonic = System.monotonic_time(:millisecond) + 50
      due_at = DateTime.add(DateTime.utc_now(), 50, :millisecond)
      scheduled = create_worker(queue_name, %{status: :scheduled, scheduled_at: due_at})

      start_queue(queue_name, concurrency: 1, check_interval: check_interval)

      # Every enqueued worker takes ~10ms, so the backlog keeps the queue busy
      # for over 600ms. The scheduled worker waits for the next lookup, at most
      # a check interval away, and for the running worker to free the only slot
      # - which, on a loaded machine, takes much longer than 10ms.
      #
      run_until_started(scheduled.id)
      waited = System.monotonic_time(:millisecond) - due_at_monotonic

      assert waited < 3 * check_interval
    end

    test "they are looked up again while they fill every free slot" do
      queue_name = "scheduled_backlog"
      past = DateTime.add(DateTime.utc_now(), -60, :second)

      scheduled_ids =
        for _ <- 1..3, do: create_worker(queue_name, %{status: :scheduled, scheduled_at: past}).id

      enqueued = create_worker(queue_name)

      start_queue(queue_name, concurrency: 1)

      started =
        for _ <- 1..4 do
          assert_receive {:started, id, pid}, 1_000
          finish(pid, :ok)
          id
        end

      assert started == scheduled_ids ++ [enqueued.id]
    end

    test "an idle queue runs a scheduled worker once it is due" do
      queue_name = "scheduled_due_while_idle"
      due_at = DateTime.add(DateTime.utc_now(), 100, :millisecond)
      scheduled = create_worker(queue_name, %{status: :scheduled, scheduled_at: due_at})

      start_queue(queue_name, concurrency: 1, check_interval: 50)

      id = scheduled.id
      assert_receive {:started, ^id, _pid}, 1_000
    end
  end

  describe "concurrency" do
    test "runs the workers in the order they were enqueued" do
      queue_name = "enqueued_order"
      ids = for _ <- 1..4, do: create_worker(queue_name).id

      start_queue(queue_name, concurrency: 4)

      assert started_ids(4) == ids
    end

    test "runs no more workers than the concurrency allows" do
      queue_name = "concurrency_limit"
      for _ <- 1..5, do: create_worker(queue_name)

      # The short interval means several checks happen while the first two
      # workers are still running: none of them may start anything else.
      #
      queue = start_queue(queue_name, concurrency: 2, check_interval: 20)

      assert [_, _] = collect_started(2)
      refute_receive {:started, _id, _pid}, 300

      assert length(processing_worker_ids(queue)) == 2
    end

    test "counts running workers against the limit on the next check" do
      queue_name = "concurrency_across_checks"
      for _ <- 1..3, do: create_worker(queue_name)

      start_queue(queue_name, concurrency: 2, check_interval: 20)

      assert [{_id, pid} | _] = collect_started(2)
      finish(pid, :ok)

      # Exactly one slot was released, so exactly one more worker may start.
      #
      assert_receive {:started, _id, _pid}, 1_000
      refute_receive {:started, _id, _pid}, 300
    end

    test "does not exceed the limit when due scheduled workers fill it" do
      # `limit: 0` used to mean "no limit", so the enqueued workers below were
      # all fetched and started after the scheduled ones had filled the limit.
      #
      queue_name = "limit_filled_by_scheduled"
      past = DateTime.add(DateTime.utc_now(), -60, :second)

      scheduled =
        for _ <- 1..2, do: create_worker(queue_name, %{status: :scheduled, scheduled_at: past})

      for _ <- 1..3, do: create_worker(queue_name)

      start_queue(queue_name, concurrency: 2, check_interval: 20)

      started = started_ids(2)
      refute_receive {:started, _id, _pid}, 300

      assert Enum.sort(started) == scheduled |> Enum.map(& &1.id) |> Enum.sort()
    end

    test "can update concurrency on the fly" do
      {:ok, less} =
        Queue.start_link(queue: "less", config: [check_interval: 5_000, concurrency: 1])

      {:ok, more} =
        Queue.start_link(queue: "more", config: [check_interval: 5_000, concurrency: 2])

      on_exit(fn -> Enum.each([less, more], &stop_queue/1) end)

      assert :sys.get_state(less).concurrency == 1
      assert :sys.get_state(more).concurrency == 2

      :ok = Queue.set_concurrency("more", 5)

      assert :sys.get_state(less).concurrency == 1
      assert :sys.get_state(more).concurrency == 5
    end

    test "fills the new slots right after concurrency is raised" do
      queue_name = "raised_concurrency"
      for _ <- 1..3, do: create_worker(queue_name)

      start_queue(queue_name, concurrency: 1)

      assert_receive {:started, _id, _pid}, 1_000
      refute_receive {:started, _id, _pid}, 300

      :ok = Queue.set_concurrency(queue_name, 3)

      assert [_, _] = collect_started(2)
    end
  end

  describe "workers stuck in a non-completed state" do
    test "runs workers left in the running status on start" do
      queue_name = "stuck_workers"
      stuck = create_worker(queue_name, %{status: :running, attempts: 1})

      start_queue(queue_name, concurrency: 5)

      assert_receive {:started, id, _pid}, 1_000
      assert id == stuck.id
    end

    test "runs stuck workers before the enqueued ones, within the concurrency" do
      queue_name = "stuck_workers_priority"

      stuck =
        for _ <- 1..3, do: create_worker(queue_name, %{status: :running, attempts: 1})

      create_worker(queue_name)

      start_queue(queue_name, concurrency: 2, check_interval: 20)

      started = started_ids(2)
      refute_receive {:started, _id, _pid}, 300

      stuck_ids = Enum.map(stuck, & &1.id)
      assert Enum.all?(started, &(&1 in stuck_ids))
    end
  end

  describe "worker process failures" do
    test "reschedules a worker whose process is killed" do
      queue_name = "killed_worker"
      [first, second] = for _ <- 1..2, do: create_worker(queue_name).id

      start_queue(queue_name, concurrency: 1)

      assert_receive {:started, ^first, pid}, 1_000

      # A killed process can not record the failure itself, so the job would
      # stay in the :running status until the next application start.
      #
      Process.exit(pid, :kill)

      assert wait_until(fn -> worker_status(first) == :retrying end)

      {:ok, killed_worker} = Repo.get(:ant_workers, first)
      assert [error] = killed_worker.errors
      assert error.error =~ "Worker process terminated"
      assert error.attempt == 1

      # The slot is released as well.
      #
      assert_receive {:started, ^second, _pid}, 1_000
    end

    test "fails a killed worker that has no attempts left" do
      queue_name = "killed_worker_without_attempts"
      worker = create_worker(queue_name, %{}, SingleAttemptWorker)

      start_queue(queue_name, concurrency: 1)

      assert_receive {:started, _id, pid}, 1_000

      Process.exit(pid, :kill)

      assert wait_until(fn -> worker_status(worker.id) == :failed end)
    end

    test "kills the workers it started when the queue itself goes down" do
      queue_name = "queue_restart"
      worker = create_worker(queue_name)

      queue = start_queue(queue_name, concurrency: 1)

      assert_receive {:started, id, worker_pid}, 1_000
      worker_ref = Process.monitor(worker_pid)

      Process.unlink(queue)
      Process.exit(queue, :kill)

      # A worker that outlives its queue is invisible to the replacement queue,
      # which finds the job in the :running status and starts a second process
      # for it - running the job twice.
      #
      assert_receive {:DOWN, ^worker_ref, :process, ^worker_pid, _reason}, 1_000
      assert worker_status(worker.id) == :running

      start_queue(queue_name, concurrency: 1)

      assert_receive {:started, ^id, _pid}, 1_000
      refute_receive {:started, ^id, _pid}, 300
    end

    test "keeps running when a worker process can not be started" do
      Mimic.copy(DynamicSupervisor)
      set_mimic_global(%{})

      queue_name = "unstartable_worker"
      worker = create_worker(queue_name)

      stub(DynamicSupervisor, :start_child, fn _supervisor, _child_spec ->
        {:error, :max_children}
      end)

      queue = start_queue(queue_name, concurrency: 1, check_interval: 20)

      # The worker is returned to its previous status instead of being left
      # as :running, and the queue survives to pick it up again.
      #
      assert wait_until(fn -> worker_status(worker.id) == :enqueued end)
      assert Process.alive?(queue)
      assert processing_worker_ids(queue) == []
    end

    test "keeps running when a worker that can not be started is deleted meanwhile" do
      Mimic.copy(DynamicSupervisor)
      set_mimic_global(%{})

      queue_name = "unstartable_deleted_worker"
      worker = create_worker(queue_name)

      stub(DynamicSupervisor, :start_child, fn _supervisor, _child_spec ->
        Ant.Workers.delete_worker(worker)

        {:error, :max_children}
      end)

      queue = start_queue(queue_name, concurrency: 1, check_interval: 20)

      assert wait_until(fn -> Repo.get(:ant_workers, worker.id) == {:error, :not_found} end)
      refute_receive {:EXIT, ^queue, _reason}, 100
      assert Process.alive?(queue)
    end
  end

  # Helpers

  defp start_queue(queue_name, config) do
    config = Keyword.put_new(config, :check_interval, 5_000)
    {:ok, queue} = Queue.start_link(queue: queue_name, config: config)

    # Stopping the queue also stops the workers it started, which would
    # otherwise stay blocked in `perform/1` and write to the database while the
    # next test is running.
    #
    on_exit(fn -> stop_queue(queue) end)

    queue
  end

  defp stop_queue(queue) do
    if Process.alive?(queue), do: GenServer.stop(queue)
  catch
    :exit, _ -> :ok
  end

  defp create_worker(queue_name, attrs \\ %{}, worker_module \\ ControlledWorker) do
    params =
      %{test_pid: self()}
      |> worker_module.build()
      |> Map.put(:queue_name, queue_name)
      |> Map.merge(attrs)
      |> Map.from_struct()

    {:ok, worker} = Repo.insert(:ant_workers, params)

    worker
  end

  defp finish(worker_pid, result), do: send(worker_pid, {:finish, result})

  # Runs `fun` right after the queue has listed the scheduled workers to start,
  # and before it marks any of them as running.
  #
  defp after_listing_scheduled(fun) do
    expect(Ant.Workers, :list_scheduled_workers, fn clauses, date_time, opts ->
      result =
        Mimic.call_original(Ant.Workers, :list_scheduled_workers, [clauses, date_time, opts])

      fun.()
      result
    end)
  end

  # Returns `{worker_id, worker_pid}` for every worker the queue has started.
  #
  defp collect_started(count) do
    Enum.map(1..count, fn _ ->
      assert_receive {:started, id, pid}, 1_000

      {id, pid}
    end)
  end

  defp started_ids(count), do: count |> collect_started() |> Enum.map(&elem(&1, 0))

  # Lets every worker that starts run for ~10ms and complete, until the worker
  # with `id` starts. Returns the ids of the workers that ran before it.
  #
  defp run_until_started(id, started \\ []) do
    assert_receive {:started, started_id, pid}, 1_000

    if started_id == id do
      Enum.reverse(started)
    else
      Process.sleep(10)
      finish(pid, :ok)

      run_until_started(id, [started_id | started])
    end
  end

  defp processing_worker_ids(queue) do
    queue
    |> :sys.get_state()
    |> Map.fetch!(:processing_workers)
    |> Map.values()
    |> Enum.sort()
  end

  defp worker_status(worker_id) do
    {:ok, worker} = Repo.get(:ant_workers, worker_id)

    worker.status
  end

  defp wait_until(fun, timeout \\ 1_000)

  defp wait_until(_fun, timeout) when timeout <= 0, do: false

  defp wait_until(fun, timeout) do
    if fun.() do
      true
    else
      Process.sleep(10)

      wait_until(fun, timeout - 10)
    end
  end
end
