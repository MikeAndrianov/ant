defmodule Ant.Application do
  @moduledoc false

  use Application

  alias Ant.Database.Setup

  @impl true
  def start(_type, _args) do
    Setup.run()

    opts = [strategy: :one_for_one, name: Ant.Supervisor]
    Supervisor.start_link(children(), opts)
  end

  def children do
    # Worker processes are supervised by the queue that started them,
    # see Ant.Queue.init/1.
    #
    base_children = [{Registry, keys: :unique, name: Ant.QueueRegistry}]

    # `start_queues: false` allows enqueuing and inspecting workers without
    # queues picking them up and running them in the background.
    # Useful in tests that rely on the state of the workers;
    # Ant.Queue can be started and stopped manually where needed.
    #
    if Application.get_env(:ant, :start_queues, true) do
      base_children ++ [{Ant.DatabaseCleaner, []} | queue_children()]
    else
      base_children
    end
  end

  defp queue_children do
    default_queues = [
      default: [
        concurrency: 5,
        check_interval: 5_000
      ]
    ]

    queues = Application.get_env(:ant, :queues, default_queues)

    Enum.map(queues, fn {queue_name, queue_config} ->
      Supervisor.child_spec({Ant.Queue, queue: queue_name, config: queue_config},
        id: {:ant_queue, queue_name}
      )
    end)
  end
end
