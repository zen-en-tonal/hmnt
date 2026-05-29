defmodule Hmnt.Integration.WorkerDbTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 15_000

  alias Hmnt.Test.{Repo, Migrations, CounterProjection}

  # ---------------------------------------------------------------------------
  # Setup: fresh in-memory table per test
  # ---------------------------------------------------------------------------

  setup do
    Migrations.drop_counters_table!()
    Migrations.create_counters_table!()

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
end
