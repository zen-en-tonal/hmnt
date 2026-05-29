Mix.Task.run("loadpaths")

Application.ensure_all_started(:hmnt)

topic = :benchmark_tenant
key = {topic, :benchmark_projection, 1}
event = %{entity_id: 1, index: 1, type: "Ping"}

:ok = Hmnt.Notifier.subscribe(topic)

Benchee.run(
  %{
    "sharding node lookup" => fn ->
      Hmnt.Sharding.node_for(key)
    end,
    "tenant notify" => fn ->
      Hmnt.Notifier.notify(topic, event)
    end
  },
  time: 1,
  warmup: 1,
  memory_time: 0
)
