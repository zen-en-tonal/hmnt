defmodule Hmnt.Worker do
  use GenServer
  alias Hmnt.Projection

  @default_suspend_after 10_000

  defstruct [
    :name,
    :projection,
    :id,
    :suspend_ref,
    :projection_state,
    :repo,
    suspend_after: @default_suspend_after
  ]

  def start_link(args) do
    GenServer.start_link(__MODULE__, args, name: via(args[:name], args[:projection], args[:id]))
  end

  def cast_event(name, projection, event) do
    {id, _idx} = Projection.identity(projection, event)
    GenServer.cast(via(name, projection, id), {:event, event})
  end

  @impl true
  def init(args) do
    state =
      %__MODULE__{
        name: args[:name],
        projection: args[:projection],
        id: args[:id],
        projection_state: Projection.initial_state(args[:projection]),
        repo: args[:repo],
        suspend_after: args[:suspend_after] || @default_suspend_after
      }
      |> schedule_suspend()

    {:ok, state, {:continue, :start_work}}
  end

  @impl true
  def handle_continue(:start_work, state) do
    snapshot = fetch_snapshot(state.repo, state.projection, state.id)
    projection_state = snapshot || state.projection_state
    last_idx = last_seen(snapshot)

    state = replay(last_idx, %{state | projection_state: projection_state})

    persist_snapshot(state.repo, state.projection_state)

    {:noreply, schedule_suspend(state)}
  end

  # :event can emit at least once, so we need to check if we've already applied it based on the event index
  @impl true
  def handle_cast({:event, event}, state) do
    {_id, idx} = Projection.identity(state.projection, event)

    last_idx = last_seen(state.projection_state)

    if idx <= last_idx do
      {:noreply, schedule_suspend(state)}
    else
      next_state = %{
        state
        | projection_state:
            Projection.handle_event(state.projection, event, state.projection_state)
      }

      persist_snapshot(next_state.repo, next_state.projection_state)

      {:noreply, schedule_suspend(next_state)}
    end
  end

  @impl true
  def handle_info(:suspend, state) do
    persist_snapshot(state.repo, state.projection_state)

    {:stop, :normal, state}
  end

  def via(name, projection, id) do
    {:via, Registry, {Hmnt.WorkerRegistry, {name, projection, id}}}
  end

  defp schedule_suspend(state) do
    if ref = state.suspend_ref do
      Process.cancel_timer(ref)
    end

    %{state | suspend_ref: Process.send_after(self(), :suspend, state.suspend_after)}
  end

  defp fetch_snapshot(repo, projection, id) do
    # TODO: determine the primary key field for the projection, maybe via a callback or convention
    repo.get_by(projection, id: id)
  end

  defp persist_snapshot(repo, projection_state) do
    # TODO: fencing for prevent stale writes via :last_event_index
    #       if multiple workers are running for the same entity
    repo.insert_or_update!(projection_state)
  end

  defp last_seen(%{last_event_index: idx}), do: idx
  defp last_seen(%{}), do: 0

  defp replay(index_from, %__MODULE__{} = state) do
    case Projection.source(state.projection, state.id, index_from, 100) do
      [] ->
        state

      events when length(events) < 100 ->
        projection_state =
          Enum.reduce(events, state.projection_state, fn event, acc ->
            Projection.handle_event(state.projection, event, acc)
          end)

        {_, last_idx} = Projection.identity(state.projection, List.last(events))
        projection_state = %{projection_state | last_event_index: last_idx}

        %{state | projection_state: projection_state}

      events ->
        projection_state =
          Enum.reduce(events, state.projection_state, fn event, acc ->
            Projection.handle_event(state.projection, event, acc)
          end)

        {_, last_idx} = Projection.identity(state.projection, List.last(events))
        projection_state = %{projection_state | last_event_index: last_idx}

        replay(last_idx, %{state | projection_state: projection_state})
    end
  end
end
