defmodule Hmnt.Test.CompositeCounterProjection do
  use Hmnt.Schema

  @primary_key false
  schema "composite_counters" do
    field(:tenant_id, :integer, primary_key: true)
    field(:entity_id, :integer, primary_key: true)
    field(:count, :integer, default: 0)
    timestamps()
  end

  @impl true
  def identity(%{tenant_id: tenant_id, entity_id: entity_id, index: idx}),
    do: {[tenant_id, entity_id], idx}

  @impl true
  def source(_entity_id, _last_idx, _limit), do: []

  @impl true
  def handle_event(%{type: "Increment"} = event, state) do
    Ecto.Changeset.change(state, %{
      tenant_id: event.tenant_id,
      entity_id: event.entity_id,
      count: state.count + 1
    })
  end

  def handle_event(_event, state), do: Ecto.Changeset.change(state)
end
