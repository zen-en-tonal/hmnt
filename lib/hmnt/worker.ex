defmodule Hmnt.Worker do
  use GenServer
  import Ecto.Query
  alias Hmnt.Projection

  @default_suspend_after 10_000
  @default_source_batch_size 100

  defstruct [
    :name,
    :projection,
    :id,
    :suspend_ref,
    :projection_state,
    :repo,
    suspend_after: @default_suspend_after,
    source_batch_size: @default_source_batch_size
  ]

  def child_spec(args) do
    %{
      id: {__MODULE__, args[:name], args[:projection], args[:id]},
      start: {__MODULE__, :start_link, [args]},
      restart: :transient
    }
  end

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
        source_batch_size: args[:source_batch_size] || @default_source_batch_size,
        suspend_after: args[:suspend_after] || @default_suspend_after,
        suspend_ref: nil
      }

    {:ok, state, {:continue, :start_work}}
  end

  @impl true
  def handle_continue(:start_work, state) do
    snapshot = fetch_snapshot(state.repo, state.projection, state.id)
    projection_state = snapshot || state.projection_state
    last_idx = last_seen(snapshot)

    state =
      replay(last_idx, %{state | projection_state: projection_state})
      |> persist_snapshot()

    {:noreply, schedule_suspend(state)}
  end

  # Events may arrive out-of-order or with gaps (at-least-once delivery).
  # - Already seen (idx <= last_idx): skip for idempotency.
  # - Continuous (idx == last_idx + 1): apply directly.
  # - Gap detected (idx > last_idx + 1): replay from source to fill missing events,
  #   then apply this event only if the replay didn't already cover it.
  @impl true
  def handle_cast({:event, event}, state) do
    {_id, idx} = Projection.identity(state.projection, event)
    last_idx = last_seen(state.projection_state)

    cond do
      idx <= last_idx ->
        {:noreply, schedule_suspend(state)}

      idx == last_idx + 1 ->
        new_projection_state =
          state.projection
          |> Projection.handle_event(event, state.projection_state)
          |> Map.put(:last_event_index, idx)

        state =
          %{state | projection_state: new_projection_state}
          |> persist_snapshot()

        {:noreply, schedule_suspend(state)}

      idx > last_idx + 1 ->
        # Gap: replay missing events from source, then apply the current event
        # if it wasn't already covered by the replay.
        state = replay(last_idx, state)
        replayed_idx = last_seen(state.projection_state)

        state =
          if idx > replayed_idx do
            new_projection_state =
              state.projection
              |> Projection.handle_event(event, state.projection_state)
              |> Map.put(:last_event_index, idx)

            %{state | projection_state: new_projection_state}
            |> persist_snapshot()
          else
            state
          end

        {:noreply, schedule_suspend(state)}
    end
  end

  @impl true
  def handle_info(:suspend, state) do
    persist_snapshot(state)
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

  defp fetch_snapshot(nil, _projection, _id), do: nil

  defp fetch_snapshot(repo, projection, id) do
    key = entity_key(projection)
    repo.get_by(projection, [{key, id}])
  end

  defp persist_snapshot(%__MODULE__{repo: nil} = state), do: state

  defp persist_snapshot(%__MODULE__{} = state) do
    repo = state.repo
    projection = state.projection_state.__struct__
    projection_state = state.projection_state

    key = entity_key(projection)
    id = state.id

    repo.transact(fn ->
      # Attempt to acquire a lock on the record for this projection ID
      # This is a simple optimistic concurrency control mechanism to prevent
      # multiple workers from overwriting each other's state when they process
      # events for the same projection ID concurrently.
      # TODO: For better performance, consider using database-specific "SELECT ... FOR UPDATE" or similar locking mechanisms if supported by the repo.
      lock =
        from(p in projection,
          where: field(p, ^key) == ^id
        )
        |> repo.one()

      if lock do
        # Record exists: check if our state is newer before updating
        if projection_state.last_event_index > lock.last_event_index do
          attrs =
            Map.from_struct(projection_state)
            |> Map.drop([:__meta__, :__struct__])

          Ecto.Changeset.change(lock, attrs)
          |> repo.update()
        else
          # Our state is stale; skip update to avoid overwriting newer data
          {:error, {:stale_state, lock}}
        end
      else
        # No existing record: insert new one
        %{projection_state | key => id}
        |> repo.insert()
      end
    end)
    |> case do
      {:ok, record} ->
        %{state | projection_state: record}

      {:error, {:stale_state, record}} ->
        %{state | projection_state: record}

      {:error, reason} ->
        IO.inspect(reason, label: "Failed to persist snapshot")
        state
    end
  end

  defp entity_key(projection) do
    projection.__schema__(:primary_key) |> hd()
  end

  defp last_seen(nil), do: 0
  defp last_seen(%{last_event_index: idx}), do: idx
  defp last_seen(%{}), do: 0

  defp replay(index_from, %__MODULE__{} = state) do
    case Projection.source(state.projection, state.id, index_from, state.source_batch_size) do
      [] ->
        state

      events when length(events) < state.source_batch_size ->
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
