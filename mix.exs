defmodule Hmnt.MixProject do
  use Mix.Project

  def project do
    [
      app: :hmnt,
      version: "0.1.0",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      dialyzer: dialyzer(),
      aliases: aliases(),
      deps: deps()
    ]
  end

  def cli do
    [
      preferred_envs: [precommit: :test]
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: mod(Mix.env())
    ]
  end

  defp mod(:test), do: {Hmnt.TestApplication, []}
  defp mod(_), do: {Hmnt.Application, []}

  defp aliases do
    [
      precommit: [
        "deps.unlock --check-unused",
        "compile --warnings-as-errors",
        "format --check-formatted",
        "xref graph --label compile-connected --fail-above 0",
        "test --warnings-as-errors",
        "credo suggest --min-priority=normal"
      ],
      test: ["cmd epmd -daemon", "test --no-start"]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:phoenix_pubsub, "~> 2.0"},
      {:ecto, "~> 3.0"},
      {:ex_hash_ring, "~> 6.0"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:benchee, "~> 1.3", only: :dev, runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:ecto_sqlite3, "~> 0.18", only: :test},
      {:local_cluster, "~> 2.0", only: :test},
      {:stream_data, "~> 1.1", only: :test}
    ]
  end

  defp dialyzer do
    [
      plt_file: {:no_warn, "priv/plts/dialyzer.plt"},
      flags: [:error_handling, :unknown]
    ]
  end
end
