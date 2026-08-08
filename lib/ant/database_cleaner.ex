defmodule Ant.DatabaseCleaner do
  @moduledoc false

  use GenServer
  alias Ant.Workers

  @default_ttl :timer.hours(24 * 14)
  @default_interval :timer.hours(1)

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    ttl = Application.get_env(:ant, :database, [])[:ttl] || @default_ttl

    if ttl == :infinity do
      :ignore
    else
      interval = min(ttl, @default_interval)
      schedule_cleanup(interval)

      {:ok, %{ttl: ttl, interval: interval}}
    end
  end

  def handle_info(:cleanup, state) do
    run(state.ttl)
    schedule_cleanup(state.interval)

    {:noreply, state}
  end

  defp schedule_cleanup(interval) do
    Process.send_after(self(), :cleanup, interval)
  end

  # Only the timestamps are read: loading whole rows meant pulling every job's
  # args and error stack traces into memory once an hour.
  #
  defp run(ttl) do
    with {:ok, workers} <- Workers.list_worker_timestamps() do
      workers
      |> Enum.filter(&expired?(&1, ttl))
      |> Enum.each(&Workers.delete_worker/1)
    end
  end

  defp expired?(worker, ttl) do
    now = DateTime.utc_now()

    DateTime.diff(now, worker.updated_at, :millisecond) > ttl and not_scheduled?(worker, now)
  end

  defp not_scheduled?(%{scheduled_at: nil}, _date_time), do: true

  defp not_scheduled?(%{scheduled_at: scheduled_at}, date_time),
    do: DateTime.compare(scheduled_at, date_time) != :gt
end
