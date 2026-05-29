defmodule DailyAggregation do
  # This module defines a projection that aggregates events on a daily basis.
  use Hmnt.Schame
  import Ecto.Query
  
  schema "daily_aggregations" do
    field :date, :date
    field :count, :integer

    # this field wants to add to schema auto
    field :last_event_index, :integer, default: 0
  end

  @impl true
  def identity(%{occured_date: date, index: idx}), do: {day(date), idx}
  def identity(_), do: nil

  @impl true
  def source(date, last_idx, limit) do
    from(
      e in "events", 
      where: e.index > ^last_idx and day(e.occured_date) == ^date, 
      order_by: [asc: e.index], 
      limit: ^limit
    )
    |> Repo.all()
  end

  @impl true
  def handle_event(event, state) do
    %{state | count: state.count + 1, date: day(event.occured_date)}
  end

  defp day(datetime) do
    DateTime.to_date(datetime)
  end
end
