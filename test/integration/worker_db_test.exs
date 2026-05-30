defmodule Hmnt.Integration.WorkerDbTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 15_000

  alias Hmnt.Test.{
    Repo,
    Migrations,
    CounterProjection,
    SourceCounterProjection,
    CompositeCounterProjection
  }

  # ---------------------------------------------------------------------------
  # Setup: fresh tables per test
  # ---------------------------------------------------------------------------

  setup do
    Migrations.drop_counters_table!()
    Migrations.create_counters_table!()
    Migrations.drop_source_counters_table!()
    Migrations.create_source_counters_table!()
    Migrations.drop_counter_events_table!()
    Migrations.create_counter_events_table!()
    Migrations.drop_composite_counters_table!()
    Migrations.create_composite_counters_table!()

    id = System.unique_integer([:positive, :monotonic])
    name = :"db_test_#{id}"

    {:ok, _} =
      start_supervised(
        {Hmnt,
         [
           name: name,
           projections: [CounterProjection],
           repo: Repo,
           suspend_after: 200
         ]},
        id: name
      )

    {:ok, name: name}
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp notify(name, entity_id, index, type \\ "Increment") do
    Hmnt.notify(name, %{entity_id: entity_id, index: index, type: type})
  end

  defp wait_for_snapshot(entity_id, retries \\ 20) do
    case Repo.get_by(CounterProjection, entity_id: entity_id) do
      nil when retries > 0 ->
        Process.sleep(50)
        wait_for_snapshot(entity_id, retries - 1)

      nil ->
        flunk("Snapshot for entity #{entity_id} was never persisted")

      record ->
        record
    end
  end

  defp wait_for_count(entity_id, expected, retries \\ 40) do
    record = Repo.get_by(CounterProjection, entity_id: entity_id)

    cond do
      record && record.count == expected ->
        record

      retries > 0 ->
        Process.sleep(50)
        wait_for_count(entity_id, expected, retries - 1)

      true ->
        actual = record && record.count
        flunk("Expected count #{expected} for entity #{entity_id}, got #{inspect(actual)}")
    end
  end

  defp wait_for_last_event_index(entity_id, expected, retries \\ 40) do
    record = Repo.get_by(CounterProjection, entity_id: entity_id)

    cond do
      record && record.last_event_index == expected ->
        record

      retries > 0 ->
        Process.sleep(50)
        wait_for_last_event_index(entity_id, expected, retries - 1)

      true ->
        actual = record && record.last_event_index

        flunk(
          "Expected last_event_index #{expected} for entity #{entity_id}, got #{inspect(actual)}"
        )
    end
  end

  defp wait_for_worker_down(name, projection, entity_id, retries \\ 40) do
    via = Hmnt.Worker.via(name, projection, entity_id)

    if GenServer.whereis(via) do
      if retries > 0 do
        Process.sleep(50)
        wait_for_worker_down(name, projection, entity_id, retries - 1)
      else
        flunk("Worker for entity #{entity_id} did not stop")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  describe "snapshot persistence" do
    test "worker persists initial state to DB after first event", %{name: name} do
      notify(name, 1, 1)
      record = wait_for_snapshot(1)
      assert record.count == 1
      assert record.entity_id == 1
    end

    test "worker persists updated count after multiple events", %{name: name} do
      notify(name, 2, 1)
      notify(name, 2, 2)
      notify(name, 2, 3)
      record = wait_for_count(2, 3)
      assert record.count == 3
      assert record.last_event_index == 3
    end

    test "events for different entities are stored independently", %{name: name} do
      notify(name, 10, 1)
      notify(name, 20, 1)
      notify(name, 10, 2)

      wait_for_count(10, 2)
      wait_for_count(20, 1)

      r10 = Repo.get_by(CounterProjection, entity_id: 10)
      r20 = Repo.get_by(CounterProjection, entity_id: 20)
      assert r10.count == 2
      assert r20.count == 1
    end

    test "validation errors are persisted and last_event_index advances", %{name: name} do
      notify(name, 70, 1, "Invalid")
      record = wait_for_last_event_index(70, 1)

      assert record.projection_status == :invalid
      assert is_map(record.last_error)
      assert record.last_error["kind"] == "validation_error"
      assert record.last_error["errors"]["count"] == ["must be greater than or equal to 0"]
      assert record.last_error_at != nil
    end

    test "invalid changeset event index is consumed and duplicate index is skipped", %{name: name} do
      notify(name, 72, 1, "Invalid")
      invalid = wait_for_last_event_index(72, 1)

      assert invalid.count == 0
      assert invalid.projection_status == :invalid
      assert invalid.last_error["kind"] == "validation_error"

      # Duplicate idx=1 must be ignored because invalid event already advanced last_event_index.
      notify(name, 72, 1, "Increment")
      Process.sleep(200)

      skipped = Repo.get_by(CounterProjection, entity_id: 72)
      assert skipped.last_event_index == 1
      assert skipped.count == 0
      assert skipped.projection_status == :invalid
    end

    test "exceptions are persisted and later valid events recover status", %{name: name} do
      notify(name, 71, 1, "Explode")
      record = wait_for_last_event_index(71, 1)

      assert record.projection_status == :invalid
      assert is_map(record.last_error)
      assert record.last_error["kind"] == "exception"
      assert record.last_error_at != nil

      notify(name, 71, 2, "Increment")
      recovered = wait_for_count(71, 1)

      assert recovered.last_event_index == 2
      assert recovered.projection_status == :healthy
      assert recovered.last_error == nil
      assert recovered.last_error_at == nil
    end
  end

  describe "snapshot loading on worker restart" do
    test "worker loads existing snapshot from DB and continues from last_event_index", %{
      name: name
    } do
      notify(name, 3, 1)
      notify(name, 3, 2)
      wait_for_count(3, 2)

      # Force worker to stop by waiting for suspend
      wait_for_worker_down(name, CounterProjection, 3)

      # Send a new event — worker will restart, load snapshot (count=2, last_idx=2), apply idx=3
      notify(name, 3, 3)
      record = wait_for_count(3, 3)
      assert record.last_event_index == 3
    end

    test "restarted worker does not re-apply already-seen events", %{name: name} do
      notify(name, 4, 1)
      wait_for_count(4, 1)
      wait_for_worker_down(name, CounterProjection, 4)

      # Re-send the same event (idx=1 <= last_event_index=1 → should be skipped)
      notify(name, 4, 1)
      Process.sleep(300)

      record = Repo.get_by(CounterProjection, entity_id: 4)
      assert record.count == 1
    end
  end

  describe "multiple tenants sharing one repo" do
    test "two tenants with different entity ids write to the same table without interfering", %{
      name: name
    } do
      id2 = System.unique_integer([:positive, :monotonic])
      name2 = :"db_test_tenant2_#{id2}"

      {:ok, _} =
        start_supervised(
          {Hmnt,
           [
             name: name2,
             projections: [CounterProjection],
             repo: Repo,
             suspend_after: 200
           ]},
          id: name2
        )

      # Shared repo/table is supported as long as each tenant persists distinct entity keys.
      notify(name, 50, 1)
      notify(name2, 51, 1)

      wait_for_count(50, 1)
      wait_for_count(51, 1)

      record50 = Repo.get_by(CounterProjection, entity_id: 50)
      record51 = Repo.get_by(CounterProjection, entity_id: 51)
      assert record50.count == 1
      assert record51.count == 1
    end

    test "tenant events do not leak to another tenant's router", %{name: name} do
      id2 = System.unique_integer([:positive, :monotonic])
      name2 = :"db_test_isolated_#{id2}"

      {:ok, _} =
        start_supervised(
          {Hmnt,
           [
             name: name2,
             projections: [CounterProjection],
             repo: Repo,
             suspend_after: 200
           ]},
          id: name2
        )

      # Send to name2, entity 99 — should NOT create a worker under `name`
      notify(name2, 99, 1)
      wait_for_count(99, 1)

      via_name1 = Hmnt.Worker.via(name, CounterProjection, 99)
      assert GenServer.whereis(via_name1) == nil
    end
  end

  describe "worker idle suspend" do
    test "worker stops after suspend_after idle period", %{name: name} do
      notify(name, 6, 1)
      wait_for_snapshot(6)

      via = Hmnt.Worker.via(name, CounterProjection, 6)
      assert GenServer.whereis(via) != nil

      wait_for_worker_down(name, CounterProjection, 6)
      assert GenServer.whereis(via) == nil
    end
  end

  describe "composite primary keys" do
    test "persists snapshots keyed by all primary key fields" do
      composite_name = :"composite_db_test_#{System.unique_integer([:positive, :monotonic])}"

      {:ok, _} =
        start_supervised(
          {Hmnt,
           [
             name: composite_name,
             projections: [CompositeCounterProjection],
             repo: Repo,
             suspend_after: 200
           ]},
          id: composite_name
        )

      Hmnt.notify(composite_name, %{tenant_id: 1, entity_id: 100, index: 1, type: "Increment"})
      Hmnt.notify(composite_name, %{tenant_id: 1, entity_id: 100, index: 2, type: "Increment"})
      Hmnt.notify(composite_name, %{tenant_id: 2, entity_id: 100, index: 1, type: "Increment"})

      wait_for_composite_count(1, 100, 2)
      wait_for_composite_count(2, 100, 1)

      first = Repo.get_by(CompositeCounterProjection, tenant_id: 1, entity_id: 100)
      second = Repo.get_by(CompositeCounterProjection, tenant_id: 2, entity_id: 100)

      assert first.count == 2
      assert first.last_event_index == 2
      assert second.count == 1
      assert second.last_event_index == 1
    end
  end

  describe "source replay from DB events" do
    test "appended DB events are replayed when notify arrives with a gap" do
      source_name = :"source_db_test_#{System.unique_integer([:positive, :monotonic])}"

      {:ok, _} =
        start_supervised(
          {Hmnt,
           [
             name: source_name,
             projections: [SourceCounterProjection],
             repo: Repo,
             suspend_after: 200
           ]},
          id: source_name
        )

      Migrations.append_counter_event!(100, 1)
      Migrations.append_counter_event!(100, 2)

      # Trigger replay path: worker starts at 0 and receives idx=2 first.
      Hmnt.notify(source_name, %{entity_id: 100, index: 2, type: "Increment"})

      record = wait_for_source_count(100, 2)
      assert record.last_event_index == 2
    end

    test "appending new DB event and notifying advances projection" do
      source_name = :"source_db_test_#{System.unique_integer([:positive, :monotonic])}"

      {:ok, _} =
        start_supervised(
          {Hmnt,
           [
             name: source_name,
             projections: [SourceCounterProjection],
             repo: Repo,
             suspend_after: 200
           ]},
          id: source_name
        )

      Migrations.append_counter_event!(101, 1)
      Hmnt.notify(source_name, %{entity_id: 101, index: 1, type: "Increment"})
      wait_for_source_count(101, 1)

      Migrations.append_counter_event!(101, 2)
      Hmnt.notify(source_name, %{entity_id: 101, index: 2, type: "Increment"})

      record = wait_for_source_count(101, 2)
      assert record.last_event_index == 2
    end
  end

  defp wait_for_source_count(entity_id, expected, retries \\ 40) do
    record = Repo.get_by(SourceCounterProjection, entity_id: entity_id)

    cond do
      record && record.count == expected ->
        record

      retries > 0 ->
        Process.sleep(50)
        wait_for_source_count(entity_id, expected, retries - 1)

      true ->
        actual = record && record.count
        flunk("Expected source count #{expected} for entity #{entity_id}, got #{inspect(actual)}")
    end
  end

  defp wait_for_composite_count(tenant_id, entity_id, expected, retries \\ 40) do
    record = Repo.get_by(CompositeCounterProjection, tenant_id: tenant_id, entity_id: entity_id)

    cond do
      record && record.count == expected ->
        record

      retries > 0 ->
        Process.sleep(50)
        wait_for_composite_count(tenant_id, entity_id, expected, retries - 1)

      true ->
        actual = record && record.count

        flunk(
          "Expected composite count #{expected} for tenant #{tenant_id} entity #{entity_id}, got #{inspect(actual)}"
        )
    end
  end
end
