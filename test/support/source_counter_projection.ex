defmodule Hmnt.Test.SourceCounterProjection do
  use Hmnt.Schema
  import Ecto.Query

  alias Hmnt.Test.Repo

  @primary_key {:entity_id, :integer, autogenerate: false}
  schema "source_counters" do
    field(:count, :integer, default: 0)
    timestamps()
  end

  @impl true
  def identity(%{entity_id: id, index: idx}), do: {id, idx}

  @impl true
  def source(entity_id, last_idx, limit) do
    from(e in "counter_events",
      where:
        field(e, :entity_id) == ^entity_id and
          field(e, :index) > ^last_idx,
      order_by: [asc: field(e, :index)],
      limit: ^limit,
      select: %{
        entity_id: field(e, :entity_id),
        index: field(e, :index),
        type: field(e, :type)
      }
    )
    |> Repo.all()
  end

  @impl true
  def handle_event(%{type: "Increment"} = event, state) do
    Ecto.Changeset.change(state, %{count: state.count + 1, entity_id: event.entity_id})
  end

  def handle_event(_event, state), do: Ecto.Changeset.change(state)
end
