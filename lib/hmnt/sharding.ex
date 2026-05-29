defmodule Hmnt.Sharding do
  use GenServer
  alias ExHashRing.Ring

  @doc false
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def node_for(term) do
    GenServer.call(__MODULE__, {:get_node, term})
  end

  @impl true
  def init(_opts) do
    :net_kernel.monitor_nodes(true, node_type: :visible)

    {:ok, ring} = Ring.start_link()

    Ring.add_node(ring, node() |> to_string())
    for n <- Node.list(), do: Ring.add_node(ring, to_string(n))

    {:ok, %{ring: ring, local_node: node()}}
  end

  @impl true
  def handle_call({:get_node, key}, _from, state) do
    state = sync_local_node(state)
    hashable = :erlang.term_to_binary(key)
    {:ok, node} = Ring.find_node(state.ring, hashable)
    # Convert back to atom for easier usage in Registry keys, etc.
    node = String.to_atom(node)
    {:reply, node, state}
  end

  # Detect local node name change (e.g. when distribution starts after init)
  defp sync_local_node(%{local_node: same} = state) when same == node(), do: state

  defp sync_local_node(%{local_node: old, ring: ring} = state) do
    Ring.remove_node(ring, old |> to_string())
    Ring.add_node(ring, node() |> to_string())
    %{state | local_node: node()}
  end

  @impl true
  def handle_info({:nodeup, node, _info}, state) do
    Ring.add_node(state.ring, to_string(node))
    {:noreply, state}
  end

  def handle_info({:nodedown, node, _info}, state) do
    Ring.remove_node(state.ring, to_string(node))
    {:noreply, state}
  end
end
