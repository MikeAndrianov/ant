defmodule Ant.Database.Adapters.MnesiaTest do
  alias Ant.Database.Adapters.Mnesia

  use ExUnit.Case
  use MnesiaTesting

  test "insert/2 inserts record" do
    assert {:ok, record} =
             Mnesia.insert(:ant_workers, %{
               worker_module: "Ant.Worker",
               status: "pending",
               args: %{a: 1},
               attempts: 0,
               errors: [],
               opts: %{}
             })

    assert record.worker_module == "Ant.Worker"
    assert record.status == "pending"
    assert record.args == %{a: 1}
    assert record.updated_at
    assert record.attempts == 0
    assert record.errors == []
    assert record.opts == %{}
  end

  test "get/2 retrieves record" do
    {:ok, inserted_record} = insert_record()

    assert {:ok, record} = Mnesia.get(:ant_workers, inserted_record.id)

    assert record.id == inserted_record.id
    assert record.worker_module == "Ant.Worker"
    assert record.status == "pending"
    assert record.args == %{b: 2}
    assert record.attempts == 0
    assert record.errors == []
    assert record.opts == %{}
  end

  test "get/2 returns not found when record does not exist" do
    assert {:error, :not_found} = Mnesia.get(:ant_workers, "non-existent-id")
  end

  test "update/3 updates record" do
    {:ok, record} = insert_record()

    assert {:ok, updated_record} =
             Mnesia.update(:ant_workers, record.id, %{status: "running", attempts: 1})

    assert updated_record.id == record.id
    assert updated_record.status == "running"
    assert updated_record.attempts == 1
    assert updated_record.updated_at
  end

  test "filter/2 filters records by one or more attributes" do
    {:ok, %{id: record_id}} = insert_record(status: "cancelled", args: %{a: 1, c: 2})

    {:ok, %{id: record_2_id}} =
      insert_record(status: "running", args: %{a: 1, b: 2, d: 4}, attempts: 1)

    assert [%{id: ^record_id}] = Mnesia.filter(:ant_workers, %{status: "cancelled"})
    assert [%{id: ^record_2_id}] = Mnesia.filter(:ant_workers, %{status: "running", attempts: 1})

    assert [%{id: ^record_2_id}] = Mnesia.filter(:ant_workers, %{args: %{b: 2}}),
           "filters by partial map"

    assert Mnesia.filter(:ant_workers, %{args: %{another_attr: 2}}) == []
  end

  test "filter/3 reads by primary key when the id is given" do
    {:ok, record} = insert_record(status: "running")
    insert_record(status: "running")

    assert [found] = Mnesia.filter(:ant_workers, %{id: record.id})
    assert found.id == record.id

    # The remaining clauses are applied to the row that was read.
    #
    assert Mnesia.filter(:ant_workers, %{id: record.id, status: "cancelled"}) == []
  end

  test "filter/3 reads through an index when an indexed column is given" do
    # :status is indexed, :attempts is not.
    #
    {:ok, record} = insert_record(status: "cancelled", attempts: 7)
    insert_record(status: "running", attempts: 7)

    assert [found] = Mnesia.filter(:ant_workers, %{status: "cancelled", attempts: 7})
    assert found.id == record.id

    assert Mnesia.filter(:ant_workers, %{status: "cancelled", attempts: 1}) == []
  end

  test "filter/3 applies the limit to indexed and scanned lookups alike" do
    for _ <- 1..5, do: insert_record(status: "running")

    assert length(Mnesia.filter(:ant_workers, %{status: "running"}, limit: 2)) == 2
    assert length(Mnesia.filter(:ant_workers, %{attempts: 0}, limit: 2)) == 2
    assert length(Mnesia.filter(:ant_workers, %{}, limit: 3)) == 3
    assert Mnesia.filter(:ant_workers, %{}, limit: 0) == []
  end

  test "select_columns/3 returns only the requested columns" do
    {:ok, record} = insert_record(status: "running")

    assert [row] = Mnesia.select_columns(:ant_workers, %{status: "running"}, [:id, :attempts])

    assert row == %{id: record.id, attempts: 0}
  end

  test "select_columns/3 keeps the columns it filtered by" do
    insert_record(status: "running")
    insert_record(status: "cancelled")

    assert [row] = Mnesia.select_columns(:ant_workers, %{status: "cancelled"}, [:id, :status])

    assert row.status == "cancelled"
  end

  test "delete/2 deletes record" do
    {:ok, record} = insert_record()

    assert :ok = Mnesia.delete(:ant_workers, record.id)
    assert Mnesia.get(:ant_workers, record.id) == {:error, :not_found}
  end

  defp insert_record(opts \\ []) do
    status = Keyword.get(opts, :status, "pending")
    args = Keyword.get(opts, :args, %{b: 2})
    attempts = Keyword.get(opts, :attempts, 0)

    Mnesia.insert(:ant_workers, %{
      worker_module: "Ant.Worker",
      status: status,
      args: args,
      attempts: attempts,
      errors: [],
      opts: %{}
    })
  end
end
