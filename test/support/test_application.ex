defmodule Hmnt.TestApplication do
  @moduledoc """
  Alternative OTP Application entry point used in test env.
  Does not start Hmnt.Supervisor as a singleton — instances are started
  per-test via `Hmnt.start_link/1`.
  """
  use Application

  @impl true
  def start(_type, _args) do
    children = [
      {Phoenix.PubSub, name: Hmnt.PubSub},
      {Hmnt.Sharding, []},
      Hmnt.Registry,
      {Registry, keys: :unique, name: Hmnt.WorkerRegistry},
      Hmnt.Test.Repo
    ]

    opts = [strategy: :one_for_one, name: Hmnt.ApplicationSupervisor]
    Supervisor.start_link(children, opts)
  end
end
