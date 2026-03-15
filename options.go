package pistachio

type Options struct {
	ConnString string   `short:"c" env:"DATABASE_URL" default:"postgres://postgres@localhost/postgres" help:"PostgreSQL connection string."`
	Password   string   `env:"PGPASSWORD" help:"PostgreSQL password."`
	Schemas    []string `short:"n" env:"PGSCHEMAS" default:"public" help:"Schemas to inspect and modify."`
}
