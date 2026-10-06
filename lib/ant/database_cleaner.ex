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

  defp run(ttl) do
    cutoff = DateTime.add(DateTime.utc_now(), -ttl, :millisecond)
    Workers.delete_expired_workers(cutoff)
  end
end
