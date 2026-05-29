defmodule Hmnt.Supervisor do
  use Supervisor

  def ds_name(tenant), do: Module.concat(Hmnt.DynamicWorkerSupervisor, tenant)

  def start_link(args) do
    name = Keyword.fetch!(args, :name)
    Supervisor.start_link(__MODULE__, args, name: Hmnt.Registry.supervisor(name))
  end

  def init(args) do
    tenant = Keyword.fetch!(args, :name)
    ds = {DynamicSupervisor, strategy: :one_for_one, name: ds_name(tenant)}

    children = [
      ds,
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
