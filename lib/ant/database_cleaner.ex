defmodule Ant.DatabaseCleaner do
  @moduledoc false

  use GenServer
  require Logger

  alias Ant.Workers

  @default_ttl :timer.hours(24 * 14)
  @default_interval :timer.hours(1)

  # A TTL this long keeps every job, the same as :infinity - and anything longer
  # reaches past the oldest date a DateTime can hold.
  #
  @max_ttl :timer.hours(24 * 365 * 5_000)

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    case ttl() do
      :infinity ->
        :ignore

      ttl ->
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

  # The TTL is in milliseconds. A float, such as `1.5 * :timer.hours(24)`, is
  # rounded: computing the cutoff with it used to raise on every cleanup, so
  # nothing was ever removed. A value that is not a positive number fails here,
  # when the application starts, rather than in every cleanup after it.
  #
  defp ttl do
    case Application.get_env(:ant, :database, [])[:ttl] || @default_ttl do
      :infinity ->
        :infinity

      ttl when is_number(ttl) and ttl > @max_ttl ->
        :infinity

      ttl when is_number(ttl) and ttl >= 1 ->
        round(ttl)

      ttl ->
        raise ArgumentError,
              "Invalid :ttl #{inspect(ttl)} in the :ant :database configuration. " <>
                "Expected a positive number of milliseconds, or :infinity."
    end
  end

  defp schedule_cleanup(interval) do
    Process.send_after(self(), :cleanup, interval)
  end

  defp run(ttl) do
    cutoff = DateTime.add(DateTime.utc_now(), -ttl, :millisecond)

    case Workers.delete_expired_workers(cutoff) do
      :ok ->
        :ok

      {:error, failures} ->
        Logger.warning(
          "Ant could not remove #{length(failures)} expired worker(s), and will try again " <>
            "on the next cleanup. First failures: #{inspect(Enum.take(failures, 5))}"
        )
    end
  end
end
