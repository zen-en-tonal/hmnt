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

  def create_source_counters_table! do
    Ecto.Adapters.SQL.query!(Repo, """
    CREATE TABLE IF NOT EXISTS source_counters (
      id       INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_id INTEGER NOT NULL UNIQUE,
      count     INTEGER NOT NULL DEFAULT 0,
      last_event_index INTEGER NOT NULL DEFAULT 0,
      inserted_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at  TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """)
  end

  def drop_source_counters_table! do
    Ecto.Adapters.SQL.query!(Repo, "DROP TABLE IF EXISTS source_counters")
  end

  def create_counter_events_table! do
    Ecto.Adapters.SQL.query!(Repo, """
    CREATE TABLE IF NOT EXISTS counter_events (
      id       INTEGER PRIMARY KEY AUTOINCREMENT,
      entity_id INTEGER NOT NULL,
      "index" INTEGER NOT NULL,
      type     TEXT NOT NULL
    )
    """)
  end

  def drop_counter_events_table! do
    Ecto.Adapters.SQL.query!(Repo, "DROP TABLE IF EXISTS counter_events")
  end

  def append_counter_event!(entity_id, index, type \\ "Increment") do
    Ecto.Adapters.SQL.query!(
      Repo,
      "INSERT INTO counter_events (entity_id, \"index\", type) VALUES (?1, ?2, ?3)",
      [entity_id, index, type]
    )
  end
end
