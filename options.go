package pistachio

type Options struct {
	ConnString string   `short:"c" env:"DATABASE_URL" default:"postgres://postgres@localhost/postgres" help:"PostgreSQL connection string. See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING"`
	Password   string   `env:"PGPASSWORD" help:"PostgreSQL password."`
	Schemas    []string `short:"n" env:"PGSCHEMAS" default:"public" help:"Schemas to inspect and modify."`
}
