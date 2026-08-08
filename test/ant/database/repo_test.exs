defmodule Ant.RepoTest do
  use Ant.TestCase, async: false
  use MnesiaTesting
  alias Ant.Repo

  describe "insert/2" do
    test "inserts a record" do
      params = %{
        worker_module: "TestWorker",
        status: :enqueued,
        queue_name: "default",
        args: %{id: 1},
        attempts: 0,
        errors: [],
        opts: %{}
      }

      assert {:ok, created_record} = Repo.insert(:ant_workers, params)
      assert created_record.worker_module == "TestWorker"
      assert created_record.status == :enqueued
    end
  end

  describe "update/3" do
    test "updates an existing record" do
      [record] = create_test_records(1)
      update_params = %{status: :completed}

      assert {:ok, updated} = Repo.update(:ant_workers, record.id, update_params)
      assert updated.status == :completed
      assert updated.worker_module == "TestWorker"
    end

    test "returns error for non-existent record" do
      assert {:error, :not_found} = Repo.update(:ant_workers, 999, %{status: :completed})
    end
  end

  describe "delete/2" do
    test "deletes an existing record" do
      [record] = create_test_records(1)
      assert :ok = Repo.delete(:ant_workers, record.id)

      assert Repo.all(:ant_workers) == []
    end

    test "returns error for non-existent record" do
      assert {:error, :not_found} = Repo.delete(:ant_workers, 999)
    end
  end

  describe "get/2" do
    test "retrieves an existing record" do
      [record] = create_test_records(1)

      assert {:ok, retrieved} = Repo.get(:ant_workers, record.id)
      assert retrieved.id == record.id
      assert retrieved.worker_module == "TestWorker"
      assert retrieved.status == :enqueued
    end

    test "returns error for non-existent record" do
      assert {:error, :not_found} = Repo.get(:ant_workers, 999)
    end
  end

  describe "filter/3" do
    test "returns all records when no clauses specified" do
      records = create_test_records(2)

      result = Repo.filter(:ant_workers, %{})

      assert length(result) == 2
      assert_lists_contain_same(result, records, :id)
    end

    test "filters records by exact match" do
      [record1, _record2] = create_test_records(2)

      result = Repo.filter(:ant_workers, %{args: record1.args})

      assert length(result) == 1
      assert hd(result).worker_module == record1.worker_module
    end

    test "filters records by multiple clauses" do
      records = create_test_records(3)

      result = Repo.filter(:ant_workers, %{status: :enqueued})

      assert length(result) == 3
      assert_lists_contain_same(result, records, :id)
    end

    test "returns empty list when no matches found" do
      create_test_records(2)

      assert Repo.filter(:ant_workers, %{status: :completed}) == []
    end

    test "respects limit option" do
      create_test_records(3)

      result = Repo.filter(:ant_workers, %{}, limit: 2)

      assert length(result) == 2
    end

    test "returns no records for non-positive limits" do
      create_test_records(2)

      assert Repo.filter(:ant_workers, %{}, limit: 0) == []
      assert Repo.filter(:ant_workers, %{}, limit: -1) == []
    end

    test "ignores non-integer limit values" do
      records = create_test_records(2)

      result = Repo.filter(:ant_workers, %{}, limit: "invalid")

      assert length(result) == 2
      assert_lists_contain_same(result, records, :id)
    end
  end

  defp create_test_records(count, opts \\ []) do
    status = Keyword.get(opts, :status, :enqueued)

    Enum.map(1..count, fn i ->
      {:ok, record} =
        Repo.insert(:ant_workers, %{
          worker_module: "TestWorker",
          status: status,
          queue_name: "default",
          args: %{id: i},
          attempts: 0,
          errors: [],
          opts: %{}
        })

      record
    end)
  end
end
