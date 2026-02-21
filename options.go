package pistachio

type Options struct {
	Host     string `short:"h" default:"localhost" env:"PGHOST" help:"<TODO>"`
	Port     uint16 `short:"p" default:"5432" env:"PGPORT" help:"<TODO>"`
	Database string `short:"d" default:"postgres" env:"PGDATABASE" help:"<TODO>"`
	User     string `short:"U" default:"postgres" env:"PGUSER" help:"<TODO>"`
	Password string `env:"PGPASSWORD" help:"<TODO>"`
	Schema   string `short:"n" env:"PGSCHEMA" help:"<TODO>"`
}
