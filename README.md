# Hmnt

The **H**u**m**an **n**a**t**ure[\[1\]](https://en.wikipedia.org/wiki/A_Treatise_of_Human_Nature)[\[2\]](https://youtu.be/ElN_4vUvTPs?si=Ni2bNoqZFE81zZ8a) bridges events to relatinal models, allowing you to easily create and manage relationships between events and models in your Elixir applications.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `hmnt` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:hmnt, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/hmnt>.

## Test database (PostgreSQL via Docker Compose)

Start PostgreSQL for tests:

```bash
docker compose up -d test-db
```

Then run tests:

```bash
mix test
```

Optional environment overrides (defaults shown):

```bash
HMNT_TEST_DB_HOST=localhost
HMNT_TEST_DB_PORT=5432
HMNT_TEST_DB_NAME=hmnt_test
HMNT_TEST_DB_USER=postgres
HMNT_TEST_DB_PASSWORD=postgres
```
