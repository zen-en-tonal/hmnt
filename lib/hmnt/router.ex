defmodule Hmnt.Router do
  use GenServer
  alias Hmnt.Projection

  defstruct [:name, :projections]

  def start_link(args) do
    name = Keyword.fetch!(args, :name)
    GenServer.start_link(__MODULE__, args, name: Hmnt.Registry.router(name))
  end

  @impl true
  def init(args) do
    state = %__MODULE__{
      name: args[:name],
      projections: args[:projections]
    }

    :ok = Hmnt.Notifier.subscribe(state.name)

    {:ok, state}
  end

  @impl true
  def handle_info({:event, event}, state) do
    for projection <- state.projections do
      case Projection.identity(projection, event) do
        nil -> :noop
        {_id, _idx} -> Hmnt.WorkerSupervisor.cast_event(state.name, projection, event)
      end
    end

    {:noreply, state}
  end
end
