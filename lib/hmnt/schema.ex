defmodule Hmnt.Schema do
  @moduledoc """
  A macro module that combines `Ecto.Schema` with the `Hmnt.Projection` behaviour.

  When you `use Hmnt.Schema`, the module gains:

  - Full `Ecto.Schema` support via the `schema/2` macro
  - `field :last_event_index, :integer, default: 0` automatically injected into
    every `schema` block
  - Projection health fields (`:projection_status`, `:last_error`, `:last_error_at`)
    automatically injected into every `schema` block
  - `@behaviour Hmnt.Projection` with a default `initial_state/0` implementation
    that returns an empty struct of the calling module

  ## Example

      defmodule MyApp.UserProjection do
        use Hmnt.Schema
        import Ecto.Query

        schema "users" do
          field :name, :string
          field :email, :string
        end

        @impl true
        def identity(%{user_id: id, index: idx}), do: {id, idx}
        def identity(_), do: nil

        @impl true
        def source(id, last_idx, limit) do
          from(e in "user_events",
            where: e.user_id == ^id and e.index > ^last_idx,
            order_by: [asc: e.index],
            limit: ^limit
          )
          |> MyApp.Repo.all()
        end

        @impl true
        def handle_event(%{type: "UserCreated", data: data}, state) do
          Ecto.Changeset.cast(state, data, [:name, :email])
        end
      end

  `identity/1` may return a scalar entity id (`{id, idx}`) or list-based
  composite id (`{[tenant_id, id], idx}`).
  """

  defmacro __using__(_opts) do
    quote do
      use Ecto.Schema
      # Shadow Ecto.Schema.schema/2 with our own version that injects system fields
      import Ecto.Schema, except: [schema: 2]
      import Hmnt.Schema, only: [schema: 2]

      @behaviour Hmnt.Projection

      @impl Hmnt.Projection
      def initial_state(), do: struct(__MODULE__, [])

      defoverridable initial_state: 0

      @before_compile Hmnt.Schema
    end
  end

  @doc false
  defmacro __before_compile__(_env) do
    quote do
      # Catch-all fallback appended after all user-defined identity/1 clauses
      def identity(_), do: nil
    end
  end

  @doc """
  Defines the Ecto schema for this projection, automatically injecting
  system projection fields.
  """
  defmacro schema(source, do: block) do
    quote do
      Ecto.Schema.schema unquote(source) do
        unquote(block)
        Ecto.Schema.field(:last_event_index, :integer, default: 0)

        Ecto.Schema.field(:projection_status, Ecto.Enum,
          values: [:healthy, :invalid],
          default: :healthy
        )

        Ecto.Schema.field(:last_error, :map)
        Ecto.Schema.field(:last_error_at, :utc_datetime)
      end
    end
  end
end
