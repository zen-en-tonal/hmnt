import Config

if config_env() == :test do
  config :hmnt, Hmnt.Test.Repo,
    database: "/tmp/hmnt_test.db",
    pool_size: 5,
    busy_timeout: 5000,
    journal_mode: :delete,
    cache_size: -64_000,
    temp_store: :memory
end
