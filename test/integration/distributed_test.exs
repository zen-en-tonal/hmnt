defmodule Hmnt.Integration.DistributedTest do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag :distributed
  @moduletag timeout: 60_000

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp rpc(node, mod, fun, args) do
    :rpc.call(node, mod, fun, args)
  end

  defp start_tenant_on(node, name, opts) do
    args = Keyword.merge([name: name, projections: [], repo: nil, suspend_after: 5_000], opts)
    # Start as a child of the stable ApplicationSupervisor to avoid the
    # rpc-handler-link problem: if started via start_link directly, the
    # rpc handler process (which exits after returning) would be the supervisor's
    # parent; when it exits :normal the supervisor (which traps exits) terminates.
    rpc(node, Supervisor, :start_child, [Hmnt.ApplicationSupervisor, {Hmnt, args}])
  end

  defp worker_alive_on?(node, tenant, projection, entity_id) do
    via_key = {tenant, projection, entity_id}

    case rpc(node, Registry, :lookup, [Hmnt.WorkerRegistry, via_key]) do
      [{_pid, _}] -> true
      _ -> false
    end
  end

  defp worker_count_on(node, tenant, projection, entity_id) do
    via_key = {tenant, projection, entity_id}

    case rpc(node, Registry, :lookup, [Hmnt.WorkerRegistry, via_key]) do
      [{pid, _}] ->
        case rpc(node, :sys, :get_state, [pid]) do
          %{projection_state: %{count: count}} -> count
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp wait_until(fun, retries \\ 30) do
    if fun.() do
      :ok
    else
      if retries > 0 do
        Process.sleep(100)
        wait_until(fun, retries - 1)
      else
        flunk("Condition never became true within timeout")
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Tests
  # ---------------------------------------------------------------------------

  describe "Hmnt.Sharding (local)" do
    setup do
      # If a previous multi-node test left peers in the ring, wait for them to disconnect
      wait_until(fn -> Node.list() == [] end)
      # Flush Sharding's mailbox so all nodedown events are processed before assertions
      Hmnt.Sharding.node_for(:flush)
      :ok
    end

    test "same key always maps to the same node" do
      n1 = Hmnt.Sharding.node_for({:proj, 42})
      n2 = Hmnt.Sharding.node_for({:proj, 42})
      assert n1 == n2
    end

    test "node_for returns the local node when no peers are connected" do
      assert Hmnt.Sharding.node_for({:any, :key}) == node()
    end

    test "different keys may map to different nodes" do
      # With a single node, all keys must map to node()
      assert Hmnt.Sharding.node_for({:proj, 1}) == node()
      assert Hmnt.Sharding.node_for({:proj, 2}) == node()
    end
  end

  describe "multi-node: sharding agreement" do
    setup do
      {:ok, cluster} = LocalCluster.start_link(2)
      {:ok, [node1, node2] = nodes} = LocalCluster.nodes(cluster)

      for n <- nodes, do: :ok = rpc(n, Application, :ensure_all_started, [:hmnt]) |> elem(0)

      # Flush Sharding's mailbox so all nodeup events are processed before tests run
      Hmnt.Sharding.node_for(:sync)

      on_exit(fn ->
        try do
          LocalCluster.stop(cluster)
        catch
          _, _ -> :ok
        end
      end)

      {:ok, node1: node1, node2: node2, nodes: nodes}
    end

    test "all nodes agree on the same owner for a given key", %{nodes: nodes} do
      results =
        Enum.map(nodes, fn n ->
          rpc(n, Hmnt.Sharding, :node_for, [{:my_proj, 42}])
        end)

      # All nodes must converge on the same answer
      assert Enum.uniq(results) |> length() == 1
    end

    test "sharding result from local node matches what the remote node returns for same key",
         %{node1: node1, node2: _node2} do
      local_result = Hmnt.Sharding.node_for({:test_proj, 99})
      remote_result = rpc(node1, Hmnt.Sharding, :node_for, [{:test_proj, 99}])
      assert local_result == remote_result
    end
  end

  describe "multi-node: worker routing" do
    setup do
      {:ok, cluster} = LocalCluster.start_link(2)
      {:ok, [node1, node2] = nodes} = LocalCluster.nodes(cluster)

      for n <- nodes, do: :ok = rpc(n, Application, :ensure_all_started, [:hmnt]) |> elem(0)

      # Flush Sharding's mailbox so all nodeup events are processed before tests run
      Hmnt.Sharding.node_for(:sync)

      on_exit(fn ->
        try do
          LocalCluster.stop(cluster)
        catch
          _, _ -> :ok
        end
      end)

      {:ok, node1: node1, node2: node2, nodes: nodes}
    end

    test "WorkerSupervisor starts worker on the sharded node", %{node1: node1, node2: node2} do
      tenant = :dist_worker_routing
      all_nodes = [node(), node1, node2]

      # Start the tenant on all 3 nodes so the worker can be routed to any of them
      for n <- all_nodes do
        start_tenant_on(n, tenant, projections: [Hmnt.Test.CounterProjection], repo: nil)
      end

      entity_id = 1

      rpc(node(), Hmnt.WorkerSupervisor, :start_child, [
        tenant,
        Hmnt.Test.CounterProjection,
        entity_id
      ])

      # Wait for the worker to appear on any node in the cluster
      wait_until(fn ->
        Enum.any?(all_nodes, &worker_alive_on?(&1, tenant, Hmnt.Test.CounterProjection, entity_id))
      end)

      # Verify the worker landed on the node Sharding currently maps the key to
      sharded_node = Hmnt.Sharding.node_for({tenant, Hmnt.Test.CounterProjection, entity_id})
      assert sharded_node in all_nodes
      assert worker_alive_on?(sharded_node, tenant, Hmnt.Test.CounterProjection, entity_id)
    end

    test "WorkerSupervisor cast_event routes to sharded node", %{node1: node1, node2: node2} do
      tenant = :dist_cast_routing
      all_nodes = [node(), node1, node2]

      for n <- all_nodes do
        start_tenant_on(n, tenant,
          projections: [Hmnt.Test.CounterProjection],
          repo: nil
        )
      end

      event = %{entity_id: 42, index: 1, type: "Increment"}

      rpc(node(), Hmnt, :notify, [tenant, event])

      # Wait for the worker to appear on any node in the cluster
      wait_until(fn ->
        Enum.any?(all_nodes, &worker_alive_on?(&1, tenant, Hmnt.Test.CounterProjection, 42))
      end)

      # Verify the event was actually applied, not just that the worker started.
      wait_until(fn ->
        Enum.any?(all_nodes, &(worker_count_on(&1, tenant, Hmnt.Test.CounterProjection, 42) == 1))
      end)

      # Verify the worker landed on the node Sharding currently maps the key to
      sharded_node = Hmnt.Sharding.node_for({tenant, Hmnt.Test.CounterProjection, 42})
      assert sharded_node in all_nodes
      assert worker_alive_on?(sharded_node, tenant, Hmnt.Test.CounterProjection, 42)
      assert worker_count_on(sharded_node, tenant, Hmnt.Test.CounterProjection, 42) == 1
    end

    test "same entity id is routed independently per tenant", %{
      node1: node1,
      node2: node2
    } do
      tenant_a = :dist_multi_a
      tenant_b = :dist_multi_b
      all_nodes = [node(), node1, node2]
      entity_id = 1

      for t <- [tenant_a, tenant_b], n <- all_nodes do
        start_tenant_on(n, t, projections: [Hmnt.Test.CounterProjection], repo: nil)
      end

      rpc(node(), Hmnt, :notify, [tenant_a, %{entity_id: entity_id, index: 1, type: "Increment"}])
      rpc(node(), Hmnt, :notify, [tenant_b, %{entity_id: entity_id, index: 1, type: "Increment"}])

      wait_until(fn ->
        Enum.any?(all_nodes, &worker_alive_on?(&1, tenant_a, Hmnt.Test.CounterProjection, entity_id))
      end)

      wait_until(fn ->
        Enum.any?(all_nodes, &worker_alive_on?(&1, tenant_b, Hmnt.Test.CounterProjection, entity_id))
      end)

      node_a = Hmnt.Sharding.node_for({tenant_a, Hmnt.Test.CounterProjection, entity_id})
      node_b = Hmnt.Sharding.node_for({tenant_b, Hmnt.Test.CounterProjection, entity_id})

      assert node_a in all_nodes
      assert node_b in all_nodes
      assert worker_count_on(node_a, tenant_a, Hmnt.Test.CounterProjection, entity_id) == 1
      assert worker_count_on(node_b, tenant_b, Hmnt.Test.CounterProjection, entity_id) == 1
    end
  end
end
