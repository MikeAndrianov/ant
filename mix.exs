defmodule Ant.MixProject do
  use Mix.Project

  def project do
    [
      app: :ant,
      package: package(),
      name: "Ant",
      description: "Background job processing library for Elixir focused on simplicity",
      version: "1.0.0",
      elixir: "~> 1.16",
      start_permanent: Mix.env() == :prod,
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      dialyzer: dialyzer()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger, :mnesia],
      mod: {Ant.Application, []}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]

  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:mimic, "~> 1.10", only: :test},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp dialyzer do
    [
      # Ant's own callers are analysed against Mnesia, which is not a
      # dependency Dialyzer picks up on its own.
      #
      plt_add_apps: [:mnesia, :mix],
      plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
      flags: [:error_handling, :extra_return, :missing_return, :underspecs]
    ]
  end

  defp package do
    [
      # Listed explicitly: the default set includes priv/, which holds the
      # local Dialyzer PLT.
      #
      files: ["lib", "mix.exs", "README.md", "CHANGELOG.md", ".formatter.exs"],
      licenses: ["MIT"],
      links: %{"GitHub" => "https://github.com/MikeAndrianov/ant"}
    ]
  end
end
