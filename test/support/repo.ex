defmodule Hmnt.Test.Repo do
  use Ecto.Repo,
    otp_app: :hmnt,
    adapter: Ecto.Adapters.Postgres
end
