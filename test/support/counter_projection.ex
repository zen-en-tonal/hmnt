defmodule Hmnt.Test.CounterProjection do
  @moduledoc """
  A projection used only in integration tests.
  Stores a per-entity event count in the `counters` SQLite table.
  """
  use Hmnt.Schema

  @primary_key {:entity_id, :integer, autogenerate: false}
  schema "counters" do
    field(:count, :integer, default: 0)
    timestamps()
  end

  @impl true
  def identity(%{entity_id: id, index: idx}), do: {id, idx}

  @impl true
  def source(_entity_id, _last_idx, _limit) do
    # Tests inject events directly via Hmnt.notify; no event store table is created.
    []
  end

  @impl true
  def handle_event(%{type: "Increment"} = event, state) do
    %{state | count: state.count + 1, entity_id: event.entity_id}
  end

  def handle_event(_event, state), do: state
end
