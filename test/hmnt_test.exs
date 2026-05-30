defmodule HmntTest do
  use ExUnit.Case, async: false

  # ---------------------------------------------------------------------------
  # Test fixtures
  # ---------------------------------------------------------------------------

  defmodule FakeRepo do
    @moduledoc "In-memory repo stub for tests"
    use Agent

    def start_link(_), do: Agent.start_link(fn -> %{} end, name: __MODULE__)

    def get_by(schema, [{key, id}]) do
      Agent.get(__MODULE__, &Map.get(&1, {schema, key, id}))
    end

    def get_by(schema, id: id), do: get_by(schema, [{:id, id}])

    def one(%Ecto.Query{from: %{source: {_table, schema}}, wheres: wheres}) do
      {key, id} = extract_lookup(wheres)

      if key && id do
        get_by(schema, [{key, id}])
      end
    end

    def one(_), do: nil

    def insert_or_update!(%Ecto.Changeset{} = cs) do
      record = Ecto.Changeset.apply_changes(cs)
      Agent.update(__MODULE__, &store_record(&1, record))
      record
    end

    def insert_or_update!(record) do
      Agent.update(__MODULE__, &store_record(&1, record))
      record
    end

    def insert(record), do: {:ok, insert_or_update!(record)}
    def update(record), do: {:ok, insert_or_update!(record)}

    def transact(fun) when is_function(fun, 0), do: fun.()

    defp primary_key(record) do
      Map.get(record, :id) || Map.get(record, :entity_id)
    end

    defp store_record(state, record) do
      schema = record.__struct__
      pk = primary_key(record)

      state
      |> Map.put({schema, :id, pk}, record)
      |> maybe_put({schema, :entity_id, Map.get(record, :entity_id)}, record)
    end

    defp maybe_put(state, {_schema, _key, nil}, _record), do: state
    defp maybe_put(state, key, record), do: Map.put(state, key, record)

    defp extract_lookup([%{params: params} | _]) do
      key = Enum.find_value(params, fn {value, _meta} -> if is_atom(value), do: value end)

      id =
        Enum.find_value(params, fn {value, _meta} ->
          if is_integer(value) or is_binary(value), do: value
        end)

      {key, id}
    end

    defp extract_lookup(_), do: {nil, nil}

    def all, do: Agent.get(__MODULE__, &Map.values/1)
    def reset, do: Agent.update(__MODULE__, fn _ -> %{} end)
  end

  defmodule CounterProjection do
    @moduledoc "Simple projection that counts events per entity"
    use Hmnt.Schema

    schema "counters" do
      field(:entity_id, :integer)
      field(:count, :integer, default: 0)
    end

    defmodule ListIdentityProjection do
      @moduledoc "Projection fixture with list-based identity"
      use Hmnt.Schema

      schema "list_identity_projection" do
        field(:tenant_id, :integer)
        field(:entity_id, :integer)
        field(:count, :integer, default: 0)
      end

      @impl true
      def identity(%{tenant_id: tenant_id, entity_id: entity_id, index: idx}),
        do: {[tenant_id, entity_id], idx}

      @impl true
      def source(_, _, _), do: []

      @impl true
      def handle_event(_, state), do: Ecto.Changeset.change(state)
    end

    @impl true
    def identity(%{entity_id: id, index: idx}), do: {id, idx}

    @impl true
    def source(_id, _last_idx, _limit), do: []

    @impl true
    def handle_event(%{entity_id: _id}, state) do
      Ecto.Changeset.change(state, %{count: state.count + 1})
    end
  end

  # ---------------------------------------------------------------------------
  # Hmnt.Schema tests
  # ---------------------------------------------------------------------------

  describe "Hmnt.Schema" do
    test "injects projection system fields with defaults" do
      s = CounterProjection.initial_state()
      assert Map.has_key?(s, :last_event_index)
      assert Map.has_key?(s, :projection_status)
      assert Map.has_key?(s, :last_error)
      assert Map.has_key?(s, :last_error_at)
      assert s.last_event_index == 0
      assert s.projection_status == :healthy
      assert s.last_error == nil
      assert s.last_error_at == nil
    end

    test "initial_state/0 returns an empty struct" do
      s = CounterProjection.initial_state()
      assert %CounterProjection{} = s
      assert s.count == 0
    end

    test "schema fields are present alongside injected field" do
      s = CounterProjection.initial_state()
      assert Map.has_key?(s, :entity_id)
      assert Map.has_key?(s, :count)
      assert Map.has_key?(s, :last_event_index)
      assert Map.has_key?(s, :projection_status)
      assert Map.has_key?(s, :last_error)
      assert Map.has_key?(s, :last_error_at)
    end

    test "implements Hmnt.Projection behaviour" do
      assert function_exported?(CounterProjection, :identity, 1)
      assert function_exported?(CounterProjection, :source, 3)
      assert function_exported?(CounterProjection, :handle_event, 2)
      assert function_exported?(CounterProjection, :initial_state, 0)
    end

    test "identity/1 returns {id, idx} for matching event" do
      assert {42, 7} = CounterProjection.identity(%{entity_id: 42, index: 7})
    end

    test "identity/1 returns nil for non-matching event (default fallback)" do
      assert nil == CounterProjection.identity(%{unrelated: true})
    end

    test "identity/1 default fallback can be overridden" do
      defmodule CustomIdentity do
        use Hmnt.Schema

        schema "things" do
          field(:value, :integer)
        end

        @impl true
        def identity(%{value: v, index: i}), do: {v, i}

        @impl true
        def source(_, _, _), do: []

        @impl true
        def handle_event(_, state), do: Ecto.Changeset.change(state)
      end

      assert {7, 3} = CustomIdentity.identity(%{value: 7, index: 3})
      # The default fallback is still invoked for non-matching patterns
      assert nil == CustomIdentity.identity(%{unrelated: true})
    end

    test "handle_event/2 increments count" do
      state = CounterProjection.initial_state()

      state =
        CounterProjection.handle_event(%{entity_id: 1}, state)
        |> Ecto.Changeset.apply_changes()

      assert state.count == 1

      state =
        CounterProjection.handle_event(%{entity_id: 1}, state)
        |> Ecto.Changeset.apply_changes()

      assert state.count == 2
    end

    test "custom initial_state/0 can be overridden" do
      defmodule WithCustomInitial do
        use Hmnt.Schema

        schema "things" do
          field(:value, :integer, default: 99)
        end

        @impl true
        def identity(_), do: nil
        @impl true
        def source(_, _, _), do: []
        @impl true
        def handle_event(_, state), do: Ecto.Changeset.change(state)

        @impl true
        def initial_state(), do: %__MODULE__{value: 42}
      end

      result = WithCustomInitial.initial_state()
      assert result.__struct__ == WithCustomInitial
      assert result.value == 42
    end
  end

  # ---------------------------------------------------------------------------
  # Hmnt.Projection delegation tests
  # ---------------------------------------------------------------------------

  describe "Hmnt.Projection" do
    test "identity/2 delegates to projection module" do
      assert {1, 5} = Hmnt.Projection.identity(CounterProjection, %{entity_id: 1, index: 5})
      assert nil == Hmnt.Projection.identity(CounterProjection, %{})
    end

    test "identity/2 supports list(term) entity ids" do
      assert {[10, 20], 7} =
               Hmnt.Projection.identity(HmntTest.CounterProjection.ListIdentityProjection, %{
                 tenant_id: 10,
                 entity_id: 20,
                 index: 7
               })
    end

    test "handle_event/3 delegates to projection module" do
      state = CounterProjection.initial_state()
      result = Hmnt.Projection.handle_event(CounterProjection, %{entity_id: 1}, state)
      assert %Ecto.Changeset{} = result
      assert result.valid?
      assert Ecto.Changeset.apply_changes(result).count == 1
    end

    test "initial_state/1 returns empty struct when not overridden" do
      assert %CounterProjection{} = Hmnt.Projection.initial_state(CounterProjection)
    end

    test "source/4 delegates to projection module" do
      assert [] = Hmnt.Projection.source(CounterProjection, 1, 0, 100)
    end
  end

  describe "Hmnt.Migration" do
    test "projection/0 expands to projection tracking columns" do
      require Hmnt.Migration
      expanded = Macro.expand(quote(do: Hmnt.Migration.projection()), __ENV__)
      code = Macro.to_string(expanded)

      assert code =~ "add(:last_event_index, :bigint, null: false, default: 0)"
      assert code =~ "add(:projection_status, :string, null: false, default: \"healthy\")"
      assert code =~ "add(:last_error, :map)"
      assert code =~ "add(:last_error_at, :utc_datetime)"
    end
  end

  # ---------------------------------------------------------------------------
  # Hmnt.Registry tests
  # ---------------------------------------------------------------------------

  describe "Hmnt.Registry" do
    setup do
      # Registry is started by the application
      :ok
    end

    test "via/2 returns a via tuple" do
      assert {:via, Registry, {Hmnt.Registry, {:tenant_x, :router}}} =
               Hmnt.Registry.via(:tenant_x, :router)
    end

    test "router/1 returns via tuple for :router key" do
      assert {:via, Registry, {Hmnt.Registry, {:t, :router}}} = Hmnt.Registry.router(:t)
    end

    test "worker_supervisor/1 returns via tuple for :worker_supervisor key" do
      assert {:via, Registry, {Hmnt.Registry, {:t, :worker_supervisor}}} =
               Hmnt.Registry.worker_supervisor(:t)
    end

    test "supervisor/1 returns via tuple for :supervisor key" do
      assert {:via, Registry, {Hmnt.Registry, {:t, :supervisor}}} = Hmnt.Registry.supervisor(:t)
    end

    test "lookup/2 returns nil when process not registered" do
      assert nil == Hmnt.Registry.lookup(:nonexistent, :router)
    end

    test "lookup/2 finds a registered process" do
      key = {:registry_test_tenant, :test_proc}
      {:ok, _} = Agent.start_link(fn -> :ok end, name: {:via, Registry, {Hmnt.Registry, key}})
      assert {pid, nil} = Hmnt.Registry.lookup(:registry_test_tenant, :test_proc)
      assert is_pid(pid)
    end
  end

  # ---------------------------------------------------------------------------
  # Hmnt.Notifier tests
  # ---------------------------------------------------------------------------

  describe "Hmnt.Notifier" do
    test "subscribe and notify delivers event to subscriber" do
      :ok = Hmnt.Notifier.subscribe(:notifier_test)
      :ok = Hmnt.Notifier.notify(:notifier_test, %{type: "Hello"})
      assert_receive {:event, %{type: "Hello"}}, 500
    end

    test "events are isolated per tenant" do
      :ok = Hmnt.Notifier.subscribe(:isolated_a)
      :ok = Hmnt.Notifier.notify(:isolated_b, %{type: "ForB"})
      refute_receive {:event, %{type: "ForB"}}, 100
    end

    test "unsubscribe stops delivery" do
      :ok = Hmnt.Notifier.subscribe(:unsub_test)
      :ok = Hmnt.Notifier.unsubscribe(:unsub_test)
      :ok = Hmnt.Notifier.notify(:unsub_test, %{type: "AfterUnsub"})
      refute_receive {:event, %{type: "AfterUnsub"}}, 100
    end
  end

  # ---------------------------------------------------------------------------
  # Multi-tenant Hmnt.start_link / Hmnt.notify tests
  # ---------------------------------------------------------------------------

  describe "Hmnt (multi-tenant)" do
    setup do
      {:ok, _} = start_supervised({FakeRepo, []})
      :ok
    end

    test "start_link/1 starts an isolated supervisor per tenant" do
      {:ok, pid_a} =
        start_supervised(
          {Hmnt, name: :mt_tenant_a, projections: [CounterProjection], repo: FakeRepo},
          id: :mt_a
        )

      {:ok, pid_b} =
        start_supervised(
          {Hmnt, name: :mt_tenant_b, projections: [CounterProjection], repo: FakeRepo},
          id: :mt_b
        )

      assert Process.alive?(pid_a)
      assert Process.alive?(pid_b)
      assert pid_a != pid_b
    end

    test "notify/2 routes to the correct tenant router" do
      {:ok, _} =
        start_supervised(
          {Hmnt, name: :route_test, projections: [CounterProjection], repo: FakeRepo}
        )

      Hmnt.Notifier.subscribe(:route_test)
      Hmnt.notify(:route_test, %{entity_id: 1, index: 1, type: "Tick"})
      assert_receive {:event, %{entity_id: 1}}, 500
    end

    test "two tenants do not receive each other's events" do
      {:ok, _} =
        start_supervised({Hmnt, name: :cross_a, projections: [CounterProjection], repo: FakeRepo},
          id: :cross_a
        )

      {:ok, _} =
        start_supervised({Hmnt, name: :cross_b, projections: [CounterProjection], repo: FakeRepo},
          id: :cross_b
        )

      Hmnt.Notifier.subscribe(:cross_a)
      Hmnt.notify(:cross_b, %{entity_id: 99, index: 1})
      refute_receive {:event, %{entity_id: 99}}, 150
    end

    test "child_spec/1 uses tenant name as id" do
      spec = Hmnt.child_spec(name: :spec_test, projections: [], repo: FakeRepo)
      assert spec.id == :spec_test
    end
  end
end
