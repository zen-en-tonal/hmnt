defmodule Hmnt.MixProject do
  use Mix.Project

  def project do
    [
      app: :hmnt,
      version: "0.1.0",
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger],
      mod: {Hmnt.Application, []}
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      # {:dep_from_hexpm, "~> 0.3.0"},
      # {:dep_from_git, git: "https://github.com/elixir-lang/my_dep.git", tag: "0.1.0"}
      {:phoenix_pubsub, "~> 2.0"},
      {:ecto, "~> 3.0"},
      {:ex_hash_ring, "~> 6.0"}
    ]
  end
end
