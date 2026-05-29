defmodule Hmnt.Integration.DistributedFaultPropertyTest do
  use ExUnit.Case, async: false

  alias Hmnt.Test.{CounterProjection, Migrations, Repo}

  @moduletag :integration
  @moduletag :distributed
  @moduletag timeout: 120_000

  defp rpc(node, mod, fun, args) do
    :rpc.call(node, mod, fun, args)
  end

  defp start_tenant_on(node, name, opts) do
    args =
      Keyword.merge(
        [name: name, projections: [CounterProjection], repo: Repo, suspend_after: 5_000],
        opts
      )

    rpc(node, Supervisor, :start_child, [Hmnt.ApplicationSupervisor, {Hmnt, args}])
  end

  defp worker_pid_on(node, tenant, entity_id) do
    case rpc(node, Registry, :lookup, [
           Hmnt.WorkerRegistry,
           {tenant, CounterProjection, entity_id}
         ]) do
      [{pid, _}] -> pid
      _ -> nil
    end
  end

  defp stored_counter(entity_id) do
    Repo.get_by(CounterProjection, entity_id: entity_id)
  end

  defp wait_until(fun, retries \\ 80) do
    if fun.() do
      :ok
    else
      if retries > 0 do
        Process.sleep(50)
        wait_until(fun, retries - 1)
      else
        flunk("Condition never became true within timeout")
      end
    end
  end

  defp assert_sharding_consensus(nodes, tenant, entity_id) do
    owners =
      Enum.map(nodes, fn n ->
        rpc(n, Hmnt.Sharding, :node_for, [{tenant, CounterProjection, entity_id}])
      end)

    assert Enum.uniq(owners) |> length() == 1
    hd(owners)
  end

  setup do
    Migrations.drop_counters_table!()
    Migrations.create_counters_table!()

    {:ok, cluster} = LocalCluster.start_link(2)
    {:ok, [node1, node2] = nodes} = LocalCluster.nodes(cluster)

    for n <- nodes, do: :ok = rpc(n, Application, :ensure_all_started, [:hmnt]) |> elem(0)

    Hmnt.Sharding.node_for(:sync)

    on_exit(fn ->
      try do
        LocalCluster.stop(cluster)
      catch
        _, _ -> :ok
      end
    end)

    {:ok, nodes: nodes, node1: node1, node2: node2}
  end

  test "recovers after worker crash during cross-node event flow", %{nodes: nodes} do
    tenant = :"fault_prop_#{System.unique_integer([:positive, :monotonic])}"
    entity_id = System.unique_integer([:positive, :monotonic])
    origin_node = hd(nodes)
    recovery_node = List.last(nodes)

    for n <- nodes do
      assert match?(
               {:ok, _},
               start_tenant_on(n, tenant, projections: [CounterProjection], repo: Repo)
             )
    end

    first_event = %{entity_id: entity_id, index: 1, type: "Increment"}
    rpc(origin_node, Hmnt, :notify, [tenant, first_event])

    wait_until(fn ->
      case stored_counter(entity_id) do
        %{last_event_index: 1, count: 1} -> true
        _ -> false
      end
    end)

    current_owner = assert_sharding_consensus(nodes, tenant, entity_id)

    if pid = worker_pid_on(current_owner, tenant, entity_id) do
      _ = rpc(current_owner, Process, :exit, [pid, :kill])
    end

    second_event = %{entity_id: entity_id, index: 2, type: "Increment"}
    rpc(recovery_node, Hmnt, :notify, [tenant, second_event])

    wait_until(fn ->
      case stored_counter(entity_id) do
        %{last_event_index: 2, count: 2} -> true
        _ -> false
      end
    end)

    assert stored_counter(entity_id).count == 2
    assert_sharding_consensus(nodes, tenant, entity_id)
  end
end
