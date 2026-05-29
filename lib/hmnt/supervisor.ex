defmodule Hmnt.Supervisor do
  use Supervisor

  def start_link(args) do
    name = Keyword.fetch!(args, :name)
    Supervisor.start_link(__MODULE__, args, name: Hmnt.Registry.supervisor(name))
  end

  def init(args) do
    children = [
      {Hmnt.Router, router_args(args)},
      {Hmnt.WorkerSupervisor, worker_supervisor_args(args)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp router_args(args) do
    [name: args[:name], projections: args[:projections]]
  end

  defp worker_supervisor_args(args) do
    [name: args[:name], repo: args[:repo], suspend_after: args[:suspend_after]]
  end
end
