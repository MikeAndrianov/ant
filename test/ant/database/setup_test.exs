defmodule Ant.Database.SetupTest do
  use ExUnit.Case

  alias Ant.Database.Setup

  @table :ant_setup_test

  setup do
    on_exit(fn ->
      :mnesia.delete_table(@table)
      Application.delete_env(:ant, :database)
    end)

    :ok
  end

  describe "persistence_strategy/0" do
    test "defaults to :ram_copies" do
      assert Setup.persistence_strategy() == :ram_copies
    end

    test "returns the configured strategy" do
      Application.put_env(:ant, :database, persistence_strategy: :ram_copies)

      assert Setup.persistence_strategy() == :ram_copies
    end

    test "raises on an unknown strategy instead of silently using RAM" do
      Application.put_env(:ant, :database, persistence_strategy: :disk_copies)

      assert_raise RuntimeError, ~r/Unknown :persistence_strategy :disk_copies/, fn ->
        Setup.persistence_strategy()
      end
    end

    test "raises when a disc strategy is used without a node name" do
      Application.put_env(:ant, :database, persistence_strategy: :disc_copies)

      # The test suite runs as :nonode@nohost, where Mnesia can not persist.
      #
      assert_raise RuntimeError, ~r/requires the app to run with a node name/, fn ->
        Setup.persistence_strategy()
      end
    end
  end

  describe "create_table!/3" do
    test "creates a missing table" do
      assert :ok =
               Setup.create_table!(@table, [attributes: [:id, :name], type: :set], :ram_copies)

      assert :mnesia.table_info(@table, :attributes) == [:id, :name]
    end

    test "is idempotent for an existing table" do
      options = [attributes: [:id, :name], type: :set]

      assert :ok = Setup.create_table!(@table, options, :ram_copies)
      assert :ok = Setup.create_table!(@table, options, :ram_copies)
    end

    test "raises when the table can not be created" do
      # :unknown is not a valid value for the :type option.
      #
      assert_raise RuntimeError, ~r/could not create the #{@table} table/, fn ->
        Setup.create_table!(@table, [attributes: [:id, :name], type: :unknown], :ram_copies)
      end
    end
  end

  describe "migrate_table!/3" do
    test "keeps a table that already matches the expected definition" do
      options = [attributes: [:id, :name], type: :set]
      :ok = Setup.create_table!(@table, options, :ram_copies)
      :mnesia.dirty_write({@table, 1, "first"})

      assert :ok = Setup.migrate_table!(@table, options, :ram_copies)

      assert :mnesia.dirty_read({@table, 1}) == [{@table, 1, "first"}]
    end

    test "adds new columns to persisted rows" do
      # A table persisted by an earlier release, before :priority was added.
      #
      :ok = Setup.create_table!(@table, [attributes: [:id, :name], type: :set], :ram_copies)
      :mnesia.dirty_write({@table, 1, "first"})

      assert :ok =
               Setup.migrate_table!(
                 @table,
                 [attributes: [:id, :name, :priority], type: :set],
                 :ram_copies
               )

      assert :mnesia.table_info(@table, :attributes) == [:id, :name, :priority]
      assert :mnesia.dirty_read({@table, 1}) == [{@table, 1, "first", nil}]
    end

    test "keeps values with their column when columns are reordered or removed" do
      :ok =
        Setup.create_table!(
          @table,
          [attributes: [:id, :name, :priority], type: :set],
          :ram_copies
        )

      :mnesia.dirty_write({@table, 1, "first", :high})

      assert :ok =
               Setup.migrate_table!(
                 @table,
                 [attributes: [:id, :priority], type: :set],
                 :ram_copies
               )

      assert :mnesia.table_info(@table, :attributes) == [:id, :priority]
      assert :mnesia.dirty_read({@table, 1}) == [{@table, 1, :high}]
    end
  end

  describe "ensure_tables_available!/0" do
    test "returns :ok once the tables the application created are loaded" do
      assert :ok = Setup.ensure_tables_available!()
    end
  end

  describe "initialize_id_counter/0" do
    test "keeps an existing counter untouched" do
      # Restored afterwards: lowering the counter would hand out ids that
      # already exist, and :ant_workers is a :set.
      #
      counter = :mnesia.dirty_read({:ant_counters, :ant_workers})
      on_exit(fn -> Enum.each(counter, &:mnesia.dirty_write/1) end)

      :mnesia.dirty_write({:ant_counters, :ant_workers, 42})

      assert :ok = Setup.initialize_id_counter()

      assert :mnesia.dirty_read({:ant_counters, :ant_workers}) ==
               [{:ant_counters, :ant_workers, 42}]
    end
  end
end
