defmodule Ant.Database.Setup do
  @moduledoc false

  # Prepares the Mnesia tables Ant needs, on every application start.
  #
  # Ant joins whatever Mnesia setup the host application has instead of owning
  # it: it never stops a running Mnesia (that would take the host's own tables
  # down with it) and only applies its own :persistence_dir when it is the one
  # starting Mnesia.

  require Logger

  @persistence_strategies [:ram_copies, :disc_copies, :disc_only_copies]
  @wait_for_tables_timeout :timer.seconds(15)

  @tables [
    ant_workers: [
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
      type: :set,
      # Queues look workers up by status, the uniqueness checker by module.
      # Without these every lookup scans the whole table, history included.
      #
      index: [:status, :worker_module]
    ],
    ant_counters: [
      attributes: [:table_name, :count],
      type: :set
    ]
  ]

  def tables, do: @tables

  def run do
    persistence_strategy = persistence_strategy()

    ensure_mnesia_started!()
    ensure_schema!(persistence_strategy)
    create_tables!(persistence_strategy)
    ensure_tables_available!()
    migrate_tables!(persistence_strategy)
    initialize_id_counter()
  end

  def persistence_strategy do
    strategy = database_config()[:persistence_strategy] || :ram_copies

    unless strategy in @persistence_strategies do
      raise "Unknown :persistence_strategy #{inspect(strategy)} for :ant. " <>
              "Expected one of #{inspect(@persistence_strategies)}."
    end

    if strategy != :ram_copies and node() == :nonode@nohost do
      raise "#{inspect(strategy)} persistence requires the app to run with a node name. " <>
              "Use --sname or --name."
    end

    strategy
  end

  def ensure_mnesia_started! do
    if :mnesia.system_info(:is_running) in [:yes, :starting] do
      warn_on_ignored_persistence_dir()
    else
      configure_persistence_dir()

      case :mnesia.start() do
        :ok -> :ok
        error -> raise "Ant could not start Mnesia: #{inspect(error)}."
      end
    end
  end

  # Disc-backed tables need a disc-backed schema. Converting the schema in place
  # works with a running Mnesia, unlike :mnesia.create_schema/1, which requires
  # stopping it first.
  #
  def ensure_schema!(:ram_copies), do: :ok

  def ensure_schema!(persistence_strategy) do
    if node() in :mnesia.table_info(:schema, :disc_copies) do
      :ok
    else
      case :mnesia.change_table_copy_type(:schema, node(), :disc_copies) do
        {:atomic, :ok} ->
          :ok

        {:aborted, {:already_exists, :schema, _node, :disc_copies}} ->
          :ok

        {:aborted, reason} ->
          raise "Ant could not create a disc-backed Mnesia schema, which " <>
                  "#{inspect(persistence_strategy)} persistence requires: #{inspect(reason)}. " <>
                  "Check that the Mnesia directory " <>
                  "(#{:mnesia.system_info(:directory)}) is writable."
      end
    end
  end

  # Creating a table on every boot is how Mnesia is meant to be used, but the
  # result has to be inspected: without it a genuine failure (unwritable
  # directory, bad node, invalid options) is indistinguishable from
  # "the table is already there" and silently leaves the data in RAM.
  #
  def create_tables!(persistence_strategy) do
    Enum.each(@tables, fn {table, options} ->
      create_table!(table, options, persistence_strategy)
    end)
  end

  def create_table!(table, options, persistence_strategy) do
    case :mnesia.create_table(table, Keyword.put(options, persistence_strategy, [node()])) do
      {:atomic, :ok} ->
        :ok

      {:aborted, {:already_exists, ^table}} ->
        :ok

      {:aborted, reason} ->
        raise "Ant could not create the #{table} table with " <>
                "#{inspect(persistence_strategy)} persistence: #{inspect(reason)}."
    end
  end

  # With disc persistence, tables load asynchronously after :mnesia.start/0;
  # without waiting, queues could query a table that is not loaded yet.
  # A timeout here also surfaces table-creation failures that would otherwise
  # silently leave the data in a RAM-only table.
  #
  def ensure_tables_available! do
    case :mnesia.wait_for_tables(Keyword.keys(@tables), @wait_for_tables_timeout) do
      :ok ->
        :ok

      error ->
        raise "Ant could not load its Mnesia tables: #{inspect(error)}. " <>
                "Check the :ant database configuration (persistence_strategy, persistence_dir)."
    end
  end

  def migrate_tables!(persistence_strategy) do
    Enum.each(@tables, fn {table, options} ->
      migrate_table!(table, options, persistence_strategy)
    end)
  end

  # `create_table/2` does not touch a table that already exists, so a table
  # persisted by an earlier release keeps its old definition. Both differences
  # that matter are reconciled here, on every boot:
  #
  #   * the columns the code expects (added, removed or reordered between
  #     releases) - old rows are rewritten to the new layout;
  #   * the configured persistence strategy - changing it in the config used to
  #     be silently ignored for an existing table.
  #
  def migrate_table!(table, options, persistence_strategy) do
    migrate_storage_type!(table, persistence_strategy)
    migrate_attributes!(table, Keyword.fetch!(options, :attributes))
    migrate_indexes!(table, Keyword.get(options, :index, []))
  end

  defp migrate_storage_type!(table, persistence_strategy) do
    case :mnesia.table_info(table, :storage_type) do
      ^persistence_strategy ->
        :ok

      current ->
        Logger.info(
          "Ant is converting the #{table} table from #{inspect(current)} " <>
            "to #{inspect(persistence_strategy)}."
        )

        case :mnesia.change_table_copy_type(table, node(), persistence_strategy) do
          {:atomic, :ok} ->
            :ok

          {:aborted, reason} ->
            raise "Ant could not convert the #{table} table from #{inspect(current)} " <>
                    "to #{inspect(persistence_strategy)}: #{inspect(reason)}."
        end
    end
  end

  defp migrate_attributes!(table, attributes) do
    case :mnesia.table_info(table, :attributes) do
      ^attributes ->
        :ok

      persisted_attributes ->
        Logger.info(
          "Ant is migrating the #{table} table from #{inspect(persisted_attributes)} " <>
            "to #{inspect(attributes)}."
        )

        transform_table!(table, persisted_attributes, attributes)
    end
  end

  # Indexes added in a later release have to be created on the existing table,
  # and ones that are no longer used dropped, so that the persisted table
  # matches what the queries expect.
  #
  defp migrate_indexes!(table, indexes) do
    attributes = :mnesia.table_info(table, :attributes)

    persisted_indexes =
      table
      |> :mnesia.table_info(:index)
      # Index positions count the record name, which is not an attribute.
      #
      |> Enum.map(&Enum.at(attributes, &1 - 2))

    Enum.each(indexes -- persisted_indexes, &add_index!(table, &1))
    Enum.each(persisted_indexes -- indexes, &delete_index!(table, &1))
  end

  defp add_index!(table, index) do
    case :mnesia.add_table_index(table, index) do
      {:atomic, :ok} ->
        :ok

      {:aborted, {:already_exists, ^table, _position}} ->
        :ok

      {:aborted, reason} ->
        raise "Ant could not index #{table}.#{index}: #{inspect(reason)}."
    end
  end

  defp delete_index!(table, index) do
    case :mnesia.del_table_index(table, index) do
      {:atomic, :ok} ->
        :ok

      {:aborted, {:no_exists, ^table, _position}} ->
        :ok

      {:aborted, reason} ->
        raise "Ant could not remove the index on #{table}.#{index}: #{inspect(reason)}."
    end
  end

  # Rows are rebuilt by column name, so columns can be added, removed or
  # reordered between releases. Columns that did not exist before are filled
  # with nil.
  #
  defp transform_table!(table, persisted_attributes, attributes) do
    transform = fn record ->
      [^table | values] = Tuple.to_list(record)

      persisted_values =
        persisted_attributes
        |> Enum.zip(values)
        |> Map.new()

      List.to_tuple([table | Enum.map(attributes, &Map.get(persisted_values, &1))])
    end

    case :mnesia.transform_table(table, transform, attributes, table) do
      {:atomic, :ok} ->
        :ok

      {:aborted, reason} ->
        raise "Ant could not migrate the #{table} table to #{inspect(attributes)}: " <>
                "#{inspect(reason)}."
    end
  end

  # Worker IDs come from a persisted counter.
  # For databases created by older versions (random IDs), start the counter
  # above any already-persisted ID to avoid collisions.
  #
  def initialize_id_counter do
    {:atomic, _result} = :mnesia.transaction(&write_initial_id_counter/0)

    :ok
  end

  defp write_initial_id_counter do
    case :mnesia.read({:ant_counters, :ant_workers}) do
      [] -> :mnesia.write({:ant_counters, :ant_workers, max_persisted_id()})
      _counter -> :ok
    end
  end

  defp max_persisted_id do
    :mnesia.foldl(fn record, acc -> record |> elem(1) |> max(acc) end, 0, :ant_workers)
  end

  defp configure_persistence_dir do
    case database_config()[:persistence_dir] do
      nil -> :ok
      dir -> Application.put_env(:mnesia, :dir, to_charlist(dir))
    end
  end

  # The :dir setting is only read when Mnesia starts, so it can not be applied
  # to a Mnesia the host application has already started.
  #
  defp warn_on_ignored_persistence_dir do
    dir = database_config()[:persistence_dir]
    current_dir = to_string(:mnesia.system_info(:directory))

    if dir && to_string(dir) != current_dir do
      Logger.warning(
        "Ant's :persistence_dir (#{dir}) is ignored because Mnesia is already running " <>
          "in #{current_dir}. Configure :mnesia, :dir before starting Mnesia instead."
      )
    end

    :ok
  end

  defp database_config, do: Application.get_env(:ant, :database, [])
end
