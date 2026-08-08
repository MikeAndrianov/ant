defmodule Ant.Database.Adapters.Mnesia do
  @moduledoc false

  # Rows are looked up in the cheapest way the given clauses allow: by primary
  # key, else through a secondary index, and only otherwise by scanning the
  # table. A scan with a limit stops as soon as it has enough rows instead of
  # materialising every matching row first.

  def get(db_table, id) do
    case transaction(fn -> read_row(db_table, id) end) do
      %{} = record -> {:ok, record}
      error -> error
    end
  end

  def filter(db_table, params, opts \\ []) do
    table_columns = get_table_columns(db_table)
    limit = Keyword.get(opts, :limit)

    if is_integer(limit) and limit <= 0 do
      []
    else
      db_table
      |> fetch_rows(params, table_columns, limit)
      |> Enum.map(&to_map(&1, table_columns))
      |> Enum.filter(&matches?(&1, params))
      |> maybe_limit(limit)
    end
  end

  # Returns only the given columns of the matching rows, so a caller that needs
  # a couple of fields does not have to load every row in full.
  #
  def select_columns(db_table, params, columns) do
    table_columns = get_table_columns(db_table)

    selected_columns =
      Enum.filter(table_columns, &(&1 in columns and is_nil(Map.get(params, &1))))

    variables =
      selected_columns
      |> Enum.with_index(1)
      |> Map.new(fn {column, index} -> {column, :"$#{index}"} end)

    pattern = match_pattern(db_table, params, table_columns, variables)
    result = [{List.to_tuple(Enum.map(selected_columns, &Map.fetch!(variables, &1)))}]

    # Columns constrained by `params` are already known, so they are not read
    # from the row but merged back into the result.
    #
    known_columns = Map.take(params, columns)

    db_table
    |> :mnesia.dirty_select([{pattern, [], result}])
    |> Enum.map(fn values ->
      selected_columns
      |> Enum.zip(Tuple.to_list(values))
      |> Map.new()
      |> Map.merge(known_columns)
    end)
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
          :id -> generate_id(db_table)
          :updated_at -> DateTime.utc_now()
          column -> params[column]
        end
      )

    row = List.to_tuple([db_table | attributes])

    with :ok <- transaction(fn -> :mnesia.write(row) end) do
      {:ok, to_map(row, table_columns)}
    end
  end

  def update(db_table, id, params) do
    transaction(fn -> update_row(db_table, id, params) end)
  end

  # An aborted transaction is reported the same way as any other failure, so
  # callers never have to know that Mnesia is behind the Repo.
  #
  def transaction(fun) do
    case :mnesia.transaction(fun) do
      {:atomic, result} -> result
      {:aborted, reason} -> {:error, {:transaction_aborted, reason}}
    end
  end

  # Takes a write lock on a key of the table, which does not have to exist: it
  # is the only way to make transactions that insert *different* rows exclude
  # each other, since a lock on a row that is not there yet locks nothing.
  #
  def lock(db_table, key) do
    :mnesia.lock({:record, db_table, key}, :write)

    :ok
  end

  def delete(db_table, id) do
    with {:ok, _record} <- get(db_table, id) do
      transaction(fn -> :mnesia.delete({db_table, id}) end)
    end
  end

  # These three run inside a transaction of the caller's choosing.
  #
  defp update_row(db_table, id, params) do
    case read_row(db_table, id) do
      {:error, :not_found} -> {:error, :not_found}
      record -> write_row(db_table, Map.merge(record, params))
    end
  end

  defp read_row(db_table, id) do
    case :mnesia.read({db_table, id}) do
      [] -> {:error, :not_found}
      [row] -> to_map(row, get_table_columns(db_table))
    end
  end

  defp write_row(db_table, record) do
    table_columns = get_table_columns(db_table)
    record = Map.put(record, :updated_at, DateTime.utc_now())
    row = List.to_tuple([db_table | Enum.map(table_columns, &Map.get(record, &1))])

    with :ok <- :mnesia.write(row) do
      {:ok, to_map(row, table_columns)}
    end
  end

  defp fetch_rows(db_table, %{id: id}, _table_columns, _limit) when not is_nil(id),
    do: transaction!(fn -> :mnesia.read({db_table, id}) end)

  # An index read has no limit: it returns every row with that value, and the
  # caller pays for all of them. So a query with a limit is scanned instead,
  # stopping as soon as it has enough rows - on an ordered_set that also means
  # the rows come back oldest id first. Only an unlimited query, which has to
  # materialise its whole result anyway, goes through the index.
  #
  defp fetch_rows(db_table, params, table_columns, nil) do
    case indexed_clause(db_table, params, table_columns) do
      {column, value} -> transaction!(fn -> :mnesia.index_read(db_table, value, column) end)
      nil -> scan(db_table, params, table_columns, nil)
    end
  end

  defp fetch_rows(db_table, params, table_columns, limit),
    do: scan(db_table, params, table_columns, limit)

  # Picks a clause Mnesia can resolve through a secondary index. The remaining
  # clauses are applied to the (much smaller) result by `matches?/2`.
  #
  defp indexed_clause(db_table, params, table_columns) do
    db_table
    |> :mnesia.table_info(:index)
    # Index positions count the record name, which is not an attribute.
    #
    |> Enum.map(&Enum.at(table_columns, &1 - 2))
    |> Enum.find_value(fn column ->
      case Map.get(params, column) do
        nil -> nil
        value -> {column, value}
      end
    end)
  end

  defp scan(db_table, params, table_columns, limit) do
    match_spec = [{match_pattern(db_table, params, table_columns), [], [:"$_"]}]

    transaction!(fn ->
      if is_integer(limit) do
        db_table
        |> :mnesia.select(match_spec, limit, :read)
        |> collect_chunks(limit, [])
      else
        :mnesia.select(db_table, match_spec)
      end
    end)
  end

  # A chunk can be smaller than the requested limit while the table still holds
  # matching rows, so chunks are read until the limit is reached.
  #
  defp collect_chunks(:"$end_of_table", _limit, rows), do: rows

  defp collect_chunks({chunk, continuation}, limit, rows) do
    rows = rows ++ chunk

    if length(rows) >= limit do
      rows
    else
      continuation
      |> :mnesia.select()
      |> collect_chunks(limit, rows)
    end
  end

  defp match_pattern(db_table, params, table_columns, variables \\ %{}) do
    List.to_tuple([
      db_table
      | Enum.map(table_columns, fn column ->
          case {Map.get(params, column), Map.get(variables, column)} do
            {nil, nil} -> :_
            {nil, variable} -> variable
            {value, _variable} -> value
          end
        end)
    ])
  end

  # A clause with a nil value matches any row, the way a wildcard does
  # in the match pattern.
  #
  defp matches?(record, params) do
    Enum.all?(params, fn
      {_column, nil} -> true
      {column, value} -> value_matches?(Map.get(record, column), value)
    end)
  end

  # Mnesia matches a map pattern against a subset of the stored map, so
  # `%{args: %{b: 2}}` matches a row whose args have more keys. Rows read by key
  # or through an index skip that matching and are checked here instead.
  #
  defp value_matches?(%{} = value, %{} = pattern) do
    Enum.all?(pattern, fn {key, pattern_value} ->
      case Map.fetch(value, key) do
        {:ok, value} -> value_matches?(value, pattern_value)
        :error -> false
      end
    end)
  end

  defp value_matches?(value, pattern), do: value == pattern

  defp transaction!(fun) do
    {:atomic, result} = :mnesia.transaction(fun)

    result
  end

  defp generate_id(db_table), do: :mnesia.dirty_update_counter(:ant_counters, db_table, 1)

  defp get_table_columns(db_table), do: :mnesia.table_info(db_table, :attributes)

  defp to_map(row, table_columns) do
    [_db_table | values] = Tuple.to_list(row)

    table_columns
    |> Enum.zip(values)
    |> Enum.into(%{})
  end

  defp maybe_limit(list, limit) when is_integer(limit) and limit > 0, do: Enum.take(list, limit)
  defp maybe_limit(_list, limit) when is_integer(limit), do: []
  defp maybe_limit(list, _), do: list
end
