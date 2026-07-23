import Config

# This file configures Ant's own dev/test environments only —
# it is not loaded when Ant is used as a dependency.

if config_env() == :test do
  # Queues would pick up and run persisted workers in the background,
  # interfering with tests that assert on worker state.
  # Tests start Ant.Queue manually where needed.
  config :ant, start_queues: false
end
