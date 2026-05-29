defmodule Hmnt.Test.Migrations do
  @moduledoc "Creates projection tables for integration tests using raw SQLite3 statements."

  alias Hmnt.Test.Repo

  @doc """
  Creates a `counters` table used by `Hmnt.Test.CounterProjection`.
  Safe to call multiple times (IF NOT EXISTS).
  """
  def create_counters_table! do
    Ecto.Adapters.SQL.query!(Repo, """
    CREATE TABLE IF NOT EXISTS counters (
      id       INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_id INTEGER NOT NULL UNIQUE,
      count     INTEGER NOT NULL DEFAULT 0,
      last_event_index INTEGER NOT NULL DEFAULT 0,
      inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """)
  end

  def drop_counters_table! do
    Ecto.Adapters.SQL.query!(Repo, "DROP TABLE IF EXISTS counters")
  end
end
