defmodule UserProjection do
  use Hmnt.Schema
  import Ecto.Query

  schema "users" do
    field :name, :string
    field :email, :string
    field :age, :integer

    timestamps()
    # this field wants to add to schema auto
    field :last_event_index, :integer, default: 0
  end

  @impl true
  def source(id, last_idx, limit) do
    from(
      u in "user_events", 
      where: u.id > ^last_idx and u.user_id == ^id, 
      order_by: [asc: u.index], 
      limit: ^limit
    )
    |> Repo.all()
  end

  @impl true
  def identity(%{id: id, index: idx}), do: {id, idx}
  def identity(%_{user_id: id, index: idx}), do: {id, idx}
  def identity(_), do: nil

  @impl true
  def handle_event(%{type: "UserCreated", data: data}, state) do
    %{state | name: data["name"], email: data["email"], age: data["age"]}
  end

  def handle_event(%{type: "UserEmailUpdated", email: email}, state) do
    %{state | email: email}
  end

  def handle_event(%{type: "UserBirthday"}, state) do
    %{state | age: state.age + 1}
  end
end
