import Config

if config_env() == :test do
  test_db_port = System.get_env("HMNT_TEST_DB_PORT", "5432") |> String.to_integer()

  config :hmnt, Hmnt.Test.Repo,
    pool: Ecto.Adapters.SQL.Sandbox,
    adapter: Ecto.Adapters.Postgres,
    hostname: System.get_env("HMNT_TEST_DB_HOST", "localhost"),
    port: test_db_port,
    database: System.get_env("HMNT_TEST_DB_NAME", "hmnt_test"),
    username: System.get_env("HMNT_TEST_DB_USER", "postgres"),
    password: System.get_env("HMNT_TEST_DB_PASSWORD", "postgres"),
    pool_size: 5,
    show_sensitive_data_on_connection_error: true
end
