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
    nodes = [node() | Node.list()]
    for n <- nodes, do: Ring.add_node(ring, n)

    {:ok, %{ring: ring}}
  end

  @impl true
  def handle_call({:get_node, key}, _from, state) do
    hashable = :erlang.term_to_binary(key)
    {:ok, node} = Ring.find_node(state.ring, hashable)
    {:reply, node, state}
  end

  @impl true
  def handle_info({:nodeup, node, _info}, state) do
    Ring.add_node(state.ring, node)
    {:noreply, state}
  end

  def handle_info({:nodedown, node, _info}, state) do
    Ring.remove_node(state.ring, node)
    {:noreply, state}
  end
end
