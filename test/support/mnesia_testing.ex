defmodule MnesiaTesting do
  @moduledoc false

  defmacro __using__(_opts) do
    quote do
      setup_all do
        MnesiaTesting.prepare()

        :ok
      end

      setup do
        on_exit(fn -> MnesiaTesting.clear_db() end)

        :ok
      end
    end
  end

  # Tables are created by Ant.Application when the app starts.
  #
  def prepare do
    :ok = :mnesia.start()
    :ok = :mnesia.wait_for_tables([:ant_workers, :ant_counters], 5000)

    clear_db()
  end

  def clear_db() do
    :mnesia.clear_table(:ant_workers)
  end
end
