defmodule Hmnt.MixProject do
  use Mix.Project

  def project do
    [
      app: :hmnt,
      version: "0.1.0",
      elixir: "~> 1.19",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      aliases: aliases(),
      deps: deps()
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
      test: ["cmd epmd -daemon", "test --no-start"]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:phoenix_pubsub, "~> 2.0"},
      {:ecto, "~> 3.0"},
      {:ex_hash_ring, "~> 6.0"},
      {:ecto_sqlite3, "~> 0.18", only: :test},
      {:local_cluster, "~> 2.0", only: :test}
    ]
  end
end
