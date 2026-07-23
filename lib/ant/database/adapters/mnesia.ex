defmodule Ant.Database.Adapters.Mnesia do
  def get(db_table, id) do
    with {:atomic, %{} = record} <-
           :mnesia.transaction(fn ->
             case :mnesia.read({db_table, id}) do
               [] ->
                 {:error, :not_found}

               [row] ->
                 table_columns = get_table_columns(db_table)

                 to_map(row, table_columns)
             end
           end) do
      {:ok, record}
    else
      {:atomic, {:error, :not_found}} ->
        {:error, :not_found}

      error ->
        error
    end
  end

  def filter(db_table, params, opts \\ []) do
    table_columns = get_table_columns(db_table)
    limit = Keyword.get(opts, :limit)

    {:atomic, records} =
      :mnesia.transaction(fn ->
        attributes = Enum.map(table_columns, &Map.get(params, &1))

        match_pattern =
          List.to_tuple([
            db_table
            | Enum.map(attributes, fn
                nil -> :_
                val -> val
              end)
          ])

        :mnesia.match_object(match_pattern)
      end)

    records
    |> Enum.map(&to_map(&1, table_columns))
    |> maybe_limit(limit)
  end

  def all(db_table, opts \\ []) do
    table_columns = get_table_columns(db_table)
    limit = Keyword.get(opts, :limit)

    {:atomic, records} =
      :mnesia.transaction(fn ->
        :mnesia.foldl(
          fn record, acc -> [to_map(record, table_columns) | acc] end,
          [],
          db_table
        )
      end)

    maybe_limit(records, limit)
  end

  def insert(db_table, params) do
    table_columns = get_table_columns(db_table)

    attributes =
      Enum.map(
        table_columns,
        fn
          :id -> generate_id()
          :updated_at -> DateTime.utc_now()
          column -> params[column]
        end
      )

    row = List.to_tuple([db_table | attributes])

    with {:atomic, :ok} <- :mnesia.transaction(fn -> :mnesia.write(row) end) do
      {:ok, to_map(row, table_columns)}
    end
  end

  def update(db_table, id, params) do
    with {:atomic, result} <-
           :mnesia.transaction(fn ->
             case :mnesia.read({db_table, id}) do
               [] ->
                 {:error, :not_found}

               [row] ->
                 table_columns = get_table_columns(db_table)

                 updated_record =
                   row
                   |> to_map(table_columns)
                   |> Map.merge(params)
                   |> Map.put(:updated_at, DateTime.utc_now())

                 attributes = Enum.map(table_columns, &Map.get(updated_record, &1))
                 updated_row = List.to_tuple([db_table | attributes])

                 with :ok <- :mnesia.write(updated_row) do
                   {:ok, to_map(updated_row, table_columns)}
                 end
             end
           end) do
      result
    end
  end

  def delete(db_table, id) do
    with {:ok, _} <- get(db_table, id),
         {:atomic, :ok} <- :mnesia.transaction(fn -> :mnesia.delete({db_table, id}) end) do
      :ok
    end
  end

  defp generate_id, do: :erlang.unique_integer([:positive])

  defp get_table_columns(db_table), do: :mnesia.table_info(db_table, :attributes)

  defp to_map(row, table_columns) do
    [_db_table | values] = Tuple.to_list(row)

    table_columns
    |> Enum.zip(values)
    |> Enum.into(%{})
  end

  defp maybe_limit(list, limit) when is_integer(limit) and limit > 0, do: Enum.take(list, limit)
  defp maybe_limit(list, _), do: list
end
