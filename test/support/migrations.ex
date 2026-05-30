defmodule Hmnt.Test.Migrations do
  @moduledoc "Creates projection tables for integration tests using raw SQL statements."

  alias Hmnt.Test.Repo

  @doc """
  Creates a `counters` table used by `Hmnt.Test.CounterProjection`.
  Safe to call multiple times (IF NOT EXISTS).
  """
  def create_counters_table! do
    Ecto.Adapters.SQL.query!(Repo, """
    CREATE TABLE IF NOT EXISTS counters (
      entity_id BIGINT PRIMARY KEY,
      count BIGINT,
      inserted_at TIMESTAMP(0),
      updated_at TIMESTAMP(0),
      last_event_index BIGINT NOT NULL DEFAULT 0,
      projection_status TEXT NOT NULL DEFAULT 'healthy',
      last_error JSONB,
      last_error_at TIMESTAMP(0)
    )
    """)
  end

  def drop_counters_table! do
    Ecto.Adapters.SQL.query!(Repo, "DROP TABLE IF EXISTS counters")
  end

  def create_source_counters_table! do
    Ecto.Adapters.SQL.query!(Repo, """
    CREATE TABLE IF NOT EXISTS source_counters (
      entity_id BIGINT PRIMARY KEY,
      count BIGINT,
      inserted_at TIMESTAMP(0),
      updated_at TIMESTAMP(0),
      last_event_index BIGINT NOT NULL DEFAULT 0,
      projection_status TEXT NOT NULL DEFAULT 'healthy',
      last_error JSONB,
      last_error_at TIMESTAMP(0)
    )
    """)
  end

  def drop_source_counters_table! do
    Ecto.Adapters.SQL.query!(Repo, "DROP TABLE IF EXISTS source_counters")
  end

  def create_counter_events_table! do
    Ecto.Adapters.SQL.query!(Repo, """
    CREATE TABLE IF NOT EXISTS counter_events (
      id BIGSERIAL PRIMARY KEY,
      entity_id BIGINT NOT NULL,
      "index" BIGINT NOT NULL,
      type TEXT NOT NULL
    )
    """)
  end

  def drop_counter_events_table! do
    Ecto.Adapters.SQL.query!(Repo, "DROP TABLE IF EXISTS counter_events")
  end

  def create_composite_counters_table! do
    Ecto.Adapters.SQL.query!(Repo, """
    CREATE TABLE IF NOT EXISTS composite_counters (
      tenant_id BIGINT NOT NULL,
      entity_id BIGINT NOT NULL,
      count BIGINT,
      inserted_at TIMESTAMP(0),
      updated_at TIMESTAMP(0),
      last_event_index BIGINT NOT NULL DEFAULT 0,
      projection_status TEXT NOT NULL DEFAULT 'healthy',
      last_error JSONB,
      last_error_at TIMESTAMP(0),
      PRIMARY KEY (tenant_id, entity_id)
    )
    """)
  end

  def drop_composite_counters_table! do
    Ecto.Adapters.SQL.query!(Repo, "DROP TABLE IF EXISTS composite_counters")
  end

  def append_counter_event!(entity_id, index, type \\ "Increment") do
    Ecto.Adapters.SQL.query!(
      Repo,
      "INSERT INTO counter_events (entity_id, \"index\", type) VALUES ($1, $2, $3)",
      [entity_id, index, type]
    )
  end
end
