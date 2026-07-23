defmodule Ant.Application do
  @moduledoc false

  use Application

  @mix_env Mix.env()

  @impl true
  def start(_type, _args) do
    start_database()

    opts = [strategy: :one_for_one, name: Ant.Supervisor]
    Supervisor.start_link(children(@mix_env), opts)
  end

  def children(:test) do
    # For test env does not start Ant.Queue GenServer
    # because it automatically pick ups workers from the database and runs them,
    # changing their state.
    # It affects the tests that rely on the state of the workers.
    # Ant.Queue should be started and stopped manually where needed.
    #
    [
      {Registry, keys: :unique, name: Ant.QueueRegistry},
      {DynamicSupervisor, name: Ant.WorkersSupervisor, strategy: :one_for_one}
    ]
  end

  def children(_env) do
    default_queues = [
      default: [
        concurrency: 5,
        check_interval: 5_000
      ]
    ]

    queues = Application.get_env(:ant, :queues, default_queues)

    queue_children =
      Enum.map(queues, fn {queue_name, queue_config} ->
        Supervisor.child_spec({Ant.Queue, queue: queue_name, config: queue_config},
          id: {:ant_queue, queue_name}
        )
      end)

    [
      {Registry, keys: :unique, name: Ant.QueueRegistry},
      {DynamicSupervisor, name: Ant.WorkersSupervisor, strategy: :one_for_one},
      {Ant.DatabaseCleaner, []}
    ] ++ queue_children
  end

  defp start_database do
    :mnesia.stop()

    persistence_dir = Application.get_env(:ant, :database, [])[:persistence_dir]

    if persistence_dir do
      Application.put_env(:mnesia, :dir, persistence_dir)
    end

    unless :mnesia.system_info(:schema_location) do
      :ok = :mnesia.create_schema([node()])
    end

    :ok = :mnesia.start()

    # unless :mnesia.table_info(:ant_workers, :attributes) do
    persistence_strategy =
      Application.get_env(:ant, :database, [])[:persistence_strategy] || :ram_copies

    persistence_options =
      case persistence_strategy do
        :disc_copies ->
          validate_mnesia_disc_copies_config()
          [disc_copies: [node()]]

        :ram_copies ->
          [ram_copies: [node()]]

        :disc_only_copies ->
          validate_mnesia_disc_copies_config()
          [disc_only_copies: [node()]]

        _ ->
          [ram_copies: [node()]]
      end

    :mnesia.create_table(
      :ant_workers,
      [
        attributes: [
          :id,
          :worker_module,
          :queue_name,
          :args,
          :status,
          :attempts,
          :scheduled_at,
          :updated_at,
          :errors,
          :opts
        ],
        type: :set
      ] ++ persistence_options
    )

    :mnesia.create_table(
      :ant_counters,
      [attributes: [:table_name, :count], type: :set] ++ persistence_options
    )

    ensure_tables_available!()
    initialize_id_counter()
  end

  # With disc persistence, tables load asynchronously after `:mnesia.start/0`;
  # without waiting, queues could query a table that is not loaded yet.
  # A timeout here also surfaces table-creation failures that would otherwise
  # silently leave the data in a RAM-only table.
  #
  defp ensure_tables_available! do
    case :mnesia.wait_for_tables([:ant_workers, :ant_counters], :timer.seconds(15)) do
      :ok ->
        :ok

      error ->
        raise "Ant could not load its Mnesia tables: #{inspect(error)}. " <>
                "Check the :ant database configuration (persistence_strategy, persistence_dir)."
    end
  end

  # Worker IDs come from a persisted counter.
  # For databases created by older versions (random IDs), start the counter
  # above any already-persisted ID to avoid collisions.
  #
  defp initialize_id_counter do
    {:atomic, _} =
      :mnesia.transaction(fn ->
        case :mnesia.read({:ant_counters, :ant_workers}) do
          [] ->
            max_id =
              :mnesia.foldl(
                fn record, acc -> record |> elem(1) |> max(acc) end,
                0,
                :ant_workers
              )

            :mnesia.write({:ant_counters, :ant_workers, max_id})

          _ ->
            :ok
        end
      end)

    :ok
  end

  defp validate_mnesia_disc_copies_config do
    if node() == :nonode@nohost do
      raise "Mnesia requires the app to run with a node name. Use --sname or --name."
    end

    :ok
  end
end
