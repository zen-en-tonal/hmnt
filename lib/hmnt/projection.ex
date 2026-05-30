defmodule Hmnt.Projection do
  @type entity_id :: any() | list(any())
  @callback identity(event :: any()) :: {entity_id(), event_index :: integer()} | nil

  @callback source(entity_id :: any(), last_event_index :: integer(), limit :: integer()) :: [
              any()
            ]

  @callback handle_event(event :: any(), state :: any()) :: Ecto.Changeset.t()

  @callback initial_state() :: any()

  @optional_callbacks initial_state: 0

  @spec identity(module(), any()) :: {entity_id(), integer()} | nil
  def identity(projection, event) do
    projection.identity(event)
  end

  @spec source(module(), any(), integer(), integer()) :: list(any())
  def source(projection, entity_id, last_event_index, limit) do
    projection.source(entity_id, last_event_index, limit)
  end

  @spec handle_event(module(), any(), any()) :: Ecto.Changeset.t()
  def handle_event(projection, event, state) do
    projection.handle_event(event, state)
  end

  @spec initial_state(module()) :: any()
  def initial_state(projection) do
    if function_exported?(projection, :initial_state, 0) do
      projection.initial_state()
    else
      struct(projection, [])
    end
  end
end
