defmodule Hmnt.Migration do
  @moduledoc """
  Macros for Hmnt-aware Ecto migrations.

  Example:

      defmodule MyApp.Repo.Migrations.CreateUsers do
        use Ecto.Migration
        import Hmnt.Migration, only: [projection: 0]

        def change do
          create table("users") do
            add :name, :string
            timestamps()
            projection()
          end
        end
      end
  """

  defmacro projection do
    quote do
      add(:last_event_index, :bigint, null: false, default: 0)
      add(:projection_status, :string, null: false, default: "healthy")
      add(:last_error, :map)
      add(:last_error_at, :utc_datetime)
    end
  end
end
