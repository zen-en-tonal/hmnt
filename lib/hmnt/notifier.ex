defmodule Hmnt.Notifier do
  def notify(name, event) do
    Phoenix.PubSub.broadcast(Hmnt.PubSub, topic(name), {:event, event})
  end

  def subscribe(name) do
    Phoenix.PubSub.subscribe(Hmnt.PubSub, topic(name))
  end

  def unsubscribe(name) do
    Phoenix.PubSub.unsubscribe(Hmnt.PubSub, topic(name))
  end

  defp topic(name), do: "hmnt:events:#{name}"
end
