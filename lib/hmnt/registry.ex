defmodule Hmnt.Registry do
  @moduledoc false

  @doc false
  def child_spec(_opts) do
    Registry.child_spec(keys: :unique, name: __MODULE__)
  end

  @doc """
  Returns a via-tuple for a process registered under `{name, key}`.
  """
  def via(name, key), do: {:via, Registry, {__MODULE__, {name, key}}}

  @doc "Via-tuple for the Router belonging to tenant `name`."
  def router(name), do: via(name, :router)

  @doc "Via-tuple for the WorkerSupervisor belonging to tenant `name`."
  def worker_supervisor(name), do: via(name, :worker_supervisor)

  @doc "Via-tuple for the top-level Supervisor belonging to tenant `name`."
  def supervisor(name), do: via(name, :supervisor)

  @doc """
  Looks up a registered process for `{name, key}`.
  Returns `{pid, value}` or `nil`.
  """
  def lookup(name, key) do
    case Registry.lookup(__MODULE__, {name, key}) do
      [entry] -> entry
      [] -> nil
    end
  end
end
