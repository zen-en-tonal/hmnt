defmodule Hmnt.Worker do
  use GenServer
  import Ecto.Query, only: [from: 2]
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

    persisted = persist_snapshot(state.repo, state.projection_state)
    state = if persisted, do: %{state | projection_state: persisted}, else: state

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

        persisted = persist_snapshot(state.repo, new_projection_state)
        final_state = persisted || new_projection_state

        {:noreply, schedule_suspend(%{state | projection_state: final_state})}

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

            persisted = persist_snapshot(state.repo, new_projection_state)
            final_state = persisted || new_projection_state
            %{state | projection_state: final_state}
          else
            state
          end

        {:noreply, schedule_suspend(state)}
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

  defp fetch_snapshot(nil, _projection, _id), do: nil

  defp fetch_snapshot(repo, projection, id) do
    key = entity_key(projection)
    repo.get_by(projection, [{key, id}])
  end

  defp persist_snapshot(nil, _projection_state), do: nil

  defp persist_snapshot(repo, projection_state) do
    projection = projection_state.__struct__
    key = entity_key(projection)

    if Map.get(projection_state, key) do
      pk_fields = projection.__schema__(:primary_key)
      # Timestamps are auto-managed by Ecto; excluding them from explicit changes
      # prevents nil values from being set (autogenerate fills them on insert/update).
      update_fields =
        projection.__schema__(:fields) -- (pk_fields ++ [:inserted_at, :updated_at])

      changes = Map.take(Map.from_struct(projection_state), update_fields)
      new_idx = Map.get(projection_state, :last_event_index, 0)

      case projection_state.__meta__.state do
        :built ->
          # New record: use force_change so Ecto includes all fields in the INSERT.
          changeset =
            Enum.reduce(changes, Ecto.Changeset.change(projection_state), fn {k, v}, cs ->
              Ecto.Changeset.force_change(cs, k, v)
            end)

          repo.insert_or_update!(changeset)

        :loaded ->
          # Existing record: only advance state when our index is strictly newer than
          # what is already stored (optimistic fencing against stale updates).
          entity_val = Map.get(projection_state, key)

          changes_with_ts = Map.put(changes, :updated_at, NaiveDateTime.utc_now())

          {rows, _} =
            repo.update_all(
              from(p in projection,
                where: field(p, ^key) == ^entity_val and p.last_event_index < ^new_idx
              ),
              set: Enum.to_list(changes_with_ts)
            )

          # Return the in-memory state when the update lands; nil signals the caller
          # that the DB was already at the same-or-newer index (stale write skipped).
          if rows > 0, do: projection_state, else: nil
      end
    else
      nil
    end
  end

  defp entity_key(projection) do
    projection.__schema__(:primary_key) |> hd()
  end

  defp last_seen(nil), do: 0
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
