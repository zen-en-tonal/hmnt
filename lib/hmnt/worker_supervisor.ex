defmodule Hmnt.WorkerSupervisor do
  use GenServer

  # Each tenant gets a GenServer that routes events to Workers. Workers are
  # supervised by a DynamicSupervisor that is a sibling child of Hmnt.Supervisor.

  defp gs_name(name), do: Module.concat(__MODULE__, name)

  def start_link(args) do
    tenant = Keyword.fetch!(args, :name)
    GenServer.start_link(__MODULE__, args, name: gs_name(tenant))
  end

  def start_child(name, projection, id) do
    node = Hmnt.Sharding.node_for({name, projection, id})
    GenServer.call({gs_name(name), node}, {:start_child, projection, id})
  end

  def cast_event(name, projection, event) do
    {id, _idx} = projection.identity(event)
    node = Hmnt.Sharding.node_for({name, projection, id})
    GenServer.cast({gs_name(name), node}, {:cast_event, projection, event})
  end

  def stop_child(name, projection, id) do
    node = Hmnt.Sharding.node_for({name, projection, id})
    GenServer.cast({gs_name(name), node}, {:stop_child, projection, id})
  end

  @impl true
  def init(args) do
    {:ok, args}
  end

  @impl true
  def handle_call({:start_child, projection, id}, _from, state) do
    case start_worker(projection, id, state) do
      {:ok, _pid} -> {:reply, :ok, state}
      {:error, {:already_started, _pid}} -> {:reply, :ok, state}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_cast({:cast_event, projection, event}, state) do
    {id, _idx} = projection.identity(event)

    # Ensure the worker is running before casting the event
    case start_worker(projection, id, state) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, _reason} -> :ok
    end

    Hmnt.Worker.cast_event(state[:name], projection, event)
    {:noreply, state}
  end

  def handle_cast({:stop_child, projection, id}, state) do
    ds = Hmnt.Supervisor.ds_name(state[:name])

    if pid = GenServer.whereis(Hmnt.Worker.via(state[:name], projection, id)) do
      DynamicSupervisor.terminate_child(ds, pid)
    end

    {:noreply, state}
  end

  defp start_worker(projection, id, state) do
    args = [
      name: state[:name],
      projection: projection,
      id: id,
      repo: state[:repo],
      suspend_after: state[:suspend_after]
    ]

    DynamicSupervisor.start_child(Hmnt.Supervisor.ds_name(state[:name]), {Hmnt.Worker, args})
  end
end
