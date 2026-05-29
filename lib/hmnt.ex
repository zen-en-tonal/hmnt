defmodule Hmnt do
  @moduledoc """
  Hmnt is a multi-tenant projection system that routes events to workers by tenant name.

      ```elixir
      # Start a tenant instance
      {:ok, _pid} = Hmnt.start_link(name: :my_tenant, projections: [MyApp.UserProjection], repo: MyApp.Repo)

      # Notify an event to a specific tenant
      Hmnt.notify(:my_tenant, %{type: "UserCreated", id: 1, index: 1, data: %{name: "Alice"}})
      ```
  """

  @doc """
  Starts an Hmnt supervisor for the given tenant.

  ## Options

  - `:name` (required) — unique atom identifying the tenant
  - `:projections` — list of projection modules to run
  - `:repo` — Ecto repo used to persist and load projection state
  - `:suspend_after` — idle milliseconds before a worker suspends (default: 10_000)
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(args) do
    Hmnt.Supervisor.start_link(args)
  end

  @doc """
  Notify the given tenant of an event.
  The event will be dispatched to the appropriate worker based on its projection and identity.
  """
  @spec notify(atom(), any()) :: :ok
  def notify(name, event) do
    Hmnt.Notifier.notify(name, event)
  end

  @doc false
  def child_spec(args) do
    %{
      id: Keyword.fetch!(args, :name),
      start: {__MODULE__, :start_link, [args]},
      type: :supervisor
    }
  end
end
