# Sample Database Tests

pistachio is checked against real-world PostgreSQL schemas, not just the
hand-written fixtures in `testdata/`. Each sample database is downloaded from
its upstream source, loaded into an isolated database, and round-tripped
through `pista dump` and `pista plan`.

## Running

```bash
make test-samples
```

The target runs `test/samples/run.sh`, which builds `pista` and drives the
check. It needs a running PostgreSQL instance (`PGHOST=localhost`,
`PGUSER=postgres`, exported by the Makefile), plus `psql`, `curl`, and network
access to the upstream hosts. The same target runs in CI as the `samples` job.

The server also needs pgvector, which the discourse sample's `halfvec` columns
are typed by and the official postgres image does not ship. compose.yaml
installs `postgresql-<major>-pgvector` from PGDG when a container starts, and
the samples CI job installs the same package into its service container, so
both keep the official image and add the extension to it. The runner checks for
it up front and says so if it is missing; recreate the container with
`docker compose down && docker compose up -d`.

To load every sample into one database for manual inspection instead of
checking them one at a time, use `make schema`.

## What the check does

The runner starts with `make clean-schema`, so a schema left behind by an
earlier run cannot make a load fail on objects that already exist. Then, for
each sample, it:

1. Runs `make reset-db`, which drops and recreates `public` and drops every
   extension. The samples that load into `public` are the only ones that can
   collide with each other; every other sample owns a schema of its own and is
   checked with `pista -n`, so what it leaves behind is invisible to the next
   sample. Extensions are the exception: they are visible whichever schema they
   sit in, and a dump that says `CREATE EXTENSION IF NOT EXISTS` does nothing
   when an earlier sample already installed that extension somewhere else,
   leaving its types and operator classes unresolvable.
2. Runs the sample's loader target to download and load the schema.
3. Runs `pista dump -n <schemas>` to capture pistachio's model of the loaded
   schema as SQL.
4. Runs `pista plan -n <schemas> <dump>` and requires the output to be
   "No changes".

The two commands exercise opposite directions of the same model. `dump` goes
catalog reader -> model -> SQL; `plan` goes parser -> model -> diff against the
catalog. If the dump plans to anything other than "No changes", the catalog
reader and the parser disagree about the schema, which is a bug regardless of
which side is wrong.

Each sample reports `PASS`, `DRIFT` (the plan was not empty), or a `FAIL` with
the failing stage (`load`, `dump`, or `plan`). Failure output is printed
indented under the sample name, and the script exits non-zero if any sample
failed.

## Samples

The sample list lives in the `SAMPLES` variable in `sample-db.mk`, which the
Makefile includes, one record per line: name, loader target, loader variables,
the schemas passed to `pista -n` (blank means `public`), and any extra
`pista plan` flags (only gitlab needs one). `make print-samples` prints it for
shell consumers, so `sample-db.mk` stays the single source of the list.

Every GitHub source is fetched at a pinned commit rather than a branch, so an
upstream schema change cannot turn CI red on its own and the object counts
below stay accurate. To move a sample to a newer upstream schema, resolve the
branch with `git ls-remote https://github.com/<owner>/<repo> <branch>`, replace
the SHA in `sample-db.mk`, and re-run `make test-samples`.

| Sample | Schemas | Source |
|---|---|---|
| chinook | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| dvdrental | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| happiness_index | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| lego | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| netflix | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| pagila | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| periodic_table | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| titanic | public | [neondatabase/postgres-sample-dbs](https://github.com/neondatabase/postgres-sample-dbs) |
| world | public | [pgFoundry dbsamples](https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/) |
| usda | public | [pgFoundry dbsamples](https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/) |
| dellstore2 | public | [pgFoundry dbsamples](https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/) |
| french_towns | public | [pgFoundry dbsamples](https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/) |
| iso_3166 | public | [pgFoundry dbsamples](https://ftp.postgresql.org/pub/projects/pgFoundry/dbsamples/) |
| northwind | public | [pthom/northwind_psql](https://github.com/pthom/northwind_psql) |
| employees | employees | [h8/employees-database](https://github.com/h8/employees-database) |
| mimiciv | mimiciv_hosp, mimiciv_icu | [MIT-LCP/mimic-code](https://github.com/MIT-LCP/mimic-code) |
| mediawiki | mediawiki | [wikimedia/mediawiki](https://github.com/wikimedia/mediawiki) |
| synapse | synapse | [element-hq/synapse](https://github.com/element-hq/synapse) |
| temporal | temporal | [temporalio/temporal](https://github.com/temporalio/temporal) |
| icingadb | icingadb | [Icinga/icingadb](https://github.com/Icinga/icingadb) |
| rt | rt | [bestpractical/rt](https://github.com/bestpractical/rt) |
| sourcegraph | sourcegraph | [sourcegraph/sourcegraph-public-snapshot](https://github.com/sourcegraph/sourcegraph-public-snapshot) |
| imdb | public | [gregrahn/join-order-benchmark](https://github.com/gregrahn/join-order-benchmark) |
| adventureworks | person, humanresources, production, purchasing, sales | [lorint/AdventureWorks-for-Postgres](https://github.com/lorint/AdventureWorks-for-Postgres) |
| clubdata | cd | [PostgreSQL Exercises](https://pgexercises.com/) |
| demodb | bookings | [postgrespro/demodb](https://github.com/postgrespro/demodb) |
| musicbrainz | musicbrainz | [metabrainz/musicbrainz-server](https://github.com/metabrainz/musicbrainz-server) |
| znuny | znuny | [znuny/Znuny](https://github.com/znuny/Znuny) |
| hive | hive | [apache/hive](https://github.com/apache/hive) |
| ranger | ranger | [apache/ranger](https://github.com/apache/ranger) |
| ambari | ambari | [apache/ambari](https://github.com/apache/ambari) |
| ovirt | ovirt | [oVirt/ovirt-engine](https://github.com/oVirt/ovirt-engine) |
| gitlab | gitlab, gitlab_partitions_static, gitlab_partitions_dynamic | [gitlabhq/gitlabhq](https://github.com/gitlabhq/gitlabhq) |
| ledgersmb | ledgersmb | [ledgersmb/LedgerSMB](https://github.com/ledgersmb/LedgerSMB) |
| koji | koji | [koji-project/koji](https://github.com/koji-project/koji) |
| kea | kea | [isc-projects/kea](https://github.com/isc-projects/kea) |
| dolphinscheduler | dolphinscheduler | [apache/dolphinscheduler](https://github.com/apache/dolphinscheduler) |
| camunda | camunda | [camunda/camunda-bpm-platform](https://github.com/camunda/camunda-bpm-platform) |
| wso2apim | wso2apim | [wso2/carbon-apimgt](https://github.com/wso2/carbon-apimgt) |
| discourse | discourse | [discourse/discourse](https://github.com/discourse/discourse) |
| icinga_director | icinga_director | [Icinga/icingaweb2-module-director](https://github.com/Icinga/icingaweb2-module-director) |
| flowable | flowable | [flowable/flowable-engine](https://github.com/flowable/flowable-engine) |
| ejabberd | ejabberd | [processone/ejabberd](https://github.com/processone/ejabberd) |
| guacamole | guacamole | [apache/guacamole-client](https://github.com/apache/guacamole-client) |

## Coverage

Object counts of the loaded schemas, as of 2026-08-08 on PostgreSQL 15.18
(16.13 for icingadb, rt, znuny, gitlab, hive, ranger, ambari, and ovirt; the
Sequences column was counted on 15.18 throughout). "Constraints" excludes
foreign keys; "Types" counts enums and domains; "Sequences" counts standalone
sequences only, since pistachio manages the sequence behind a serial or
identity column as an attribute of that column rather than as an object of its
own. Counting those too would add 1,996 more, 886 of them gitlab's. All counts
are limited to the schemas the sample is checked with, and exclude what an
extension owns: the two views `pg_stat_statements` adds to sourcegraph's schema
are not sourcegraph's schema and pistachio does not read them either.

| Sample | Tables | Columns | Indexes | FKs | Constraints | Views | Types | Sequences |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| chinook | 11 | 64 | 21 | 11 | 11 | 0 | 0 | 0 |
| dvdrental | 15 | 86 | 32 | 18 | 16 | 7 | 2 | 13 |
| happiness_index | 1 | 9 | 1 | 0 | 1 | 0 | 0 | 0 |
| lego | 8 | 28 | 6 | 0 | 6 | 0 | 0 | 0 |
| netflix | 1 | 12 | 1 | 0 | 1 | 0 | 0 | 0 |
| pagila | 22 | 129 | 55 | 36 | 23 | 8 | 3 | 13 |
| periodic_table | 1 | 28 | 1 | 0 | 1 | 0 | 0 | 0 |
| titanic | 1 | 21 | 1 | 0 | 1 | 0 | 0 | 0 |
| world | 3 | 24 | 3 | 2 | 4 | 0 | 0 | 0 |
| usda | 10 | 67 | 15 | 10 | 9 | 0 | 0 | 0 |
| dellstore2 | 8 | 52 | 11 | 3 | 5 | 0 | 0 | 0 |
| french_towns | 3 | 14 | 9 | 2 | 9 | 0 | 0 | 0 |
| iso_3166 | 2 | 7 | 2 | 1 | 2 | 0 | 0 | 0 |
| northwind | 14 | 92 | 14 | 13 | 14 | 0 | 0 | 0 |
| employees | 6 | 24 | 9 | 6 | 6 | 0 | 1 | 0 |
| mimiciv | 31 | 342 | 67 | 51 | 24 | 0 | 0 | 0 |
| mediawiki | 64 | 389 | 192 | 0 | 60 | 0 | 1 | 0 |
| synapse | 134 | 624 | 236 | 14 | 86 | 0 | 0 | 10 |
| temporal | 37 | 217 | 44 | 0 | 40 | 0 | 0 | 0 |
| icingadb | 66 | 634 | 179 | 7 | 74 | 0 | 13 | 0 |
| rt | 38 | 407 | 88 | 1 | 38 | 0 | 0 | 32 |
| sourcegraph | 180 | 1,715 | 453 | 362 | 273 | 18 | 8 | 1 |
| imdb | 21 | 108 | 44 | 0 | 21 | 0 | 0 | 0 |
| adventureworks | 68 | 456 | 71 | 90 | 157 | 21 | 0 | 0 |
| clubdata | 3 | 19 | 10 | 3 | 3 | 0 | 0 | 0 |
| demodb | 9 | 45 | 15 | 8 | 21 | 3 | 0 | 0 |
| musicbrainz | 374 | 2,469 | 907 | 770 | 1,032 | 10 | 7 | 0 |
| znuny | 126 | 1,103 | 326 | 286 | 190 | 0 | 0 | 0 |
| hive | 84 | 546 | 141 | 55 | 91 | 0 | 0 | 0 |
| ranger | 85 | 889 | 305 | 213 | 130 | 1 | 0 | 84 |
| ambari | 113 | 693 | 172 | 124 | 140 | 0 | 0 | 0 |
| ovirt | 152 | 1,386 | 377 | 165 | 167 | 1 | 0 | 9 |
| gitlab | 1,422 | 14,293 | 6,353 | 2,325 | 3,949 | 15 | 0 | 8 |
| ledgersmb | 158 | 950 | 263 | 247 | 265 | 1 | 0 | 2 |
| koji | 68 | 415 | 150 | 183 | 159 | 0 | 0 | 0 |
| kea | 64 | 475 | 172 | 73 | 71 | 0 | 0 | 0 |
| dolphinscheduler | 64 | 623 | 149 | 0 | 80 | 0 | 0 | 30 |
| camunda | 49 | 681 | 281 | 42 | 56 | 0 | 0 | 0 |
| wso2apim | 247 | 1,495 | 421 | 168 | 353 | 0 | 0 | 104 |
| discourse | 354 | 3,173 | 1,114 | 23 | 336 | 1 | 2 | 0 |
| icinga_director | 121 | 872 | 381 | 171 | 236 | 0 | 21 | 0 |
| flowable | 62 | 844 | 222 | 60 | 66 | 0 | 0 | 0 |
| ejabberd | 42 | 258 | 78 | 5 | 15 | 0 | 0 | 0 |
| guacamole | 23 | 104 | 61 | 30 | 29 | 0 | 5 | 0 |
| **Total** | **4,365** | **36,882** | **13,453** | **5,578** | **8,271** | **86** | **61** | **306** |

The 44 dumps come to about 81,100 lines of SQL, and gitlab is 27,600 of them.
It is still nearly half of the indexes and constraints, a third of the tables,
and about 40 percent of the columns and foreign keys; musicbrainz, discourse,
and wso2apim are the largest of what remains. gitlab is also why
`clean-schema` drops tables a batch at a time rather than cascading through
`DROP SCHEMA`: a single statement takes locks on every object it reaches, and
gitlab's 1,422 tables and their indexes run the server out of lock table space
at the default `max_locks_per_transaction`. It is why `reset-db` resets only
`public` between samples, too.

Beyond size, the samples bring in shapes the hand-written fixtures do not
always reach: partial and expression indexes and gin, gist, hash, and brin
methods (musicbrainz, plus 12 partial and 2 gin indexes in synapse), exclusion
constraints and unlogged tables (demodb, which needs `btree_gist`), enums and
domains (dvdrental, pagila, employees, mediawiki, and icingadb, whose 13 types
are 6 enums and 7 domains, each domain carrying a named CHECK, plus
icinga_director, whose 20 enums are more than any other sample and whose one
domain carries two anonymous CHECKs, and guacamole's 5 enums), foreign keys
that all declare their referential actions (all 171 of icinga_director's name
both ON UPDATE and ON DELETE, in six combinations), unique indexes
over an expression and a gin index over `to_tsvector` (rt), columns typed by a
contrib extension (sourcegraph, with 49 `citext` columns, and six extensions
installed at once), materialized views (adventureworks, pagila), tsvector
columns (dvdrental, pagila), a non-default
collation (musicbrainz), composite types (ovirt declares 10 of them, more than
any other sample, and sourcegraph 2), standalone sequences rather than serial
columns (ranger, whose 85 tables come with 84 of them, and wso2apim, which
mixes 104 of them in with serial columns), a schema written
entirely in quoted mixed-case identifiers, so every name is case-sensitive
(hive's 84 tables, where chinook has 11), an index-heavy schema of 192 indexes
over 64 tables (mediawiki), partitioned tables at scale (gitlab declares 100 of them and
attaches 2,054 partitions, all of which live in schemas of their own), table
inheritance (ledgersmb attaches 21 children with INHERITS, the only sample that
does), four unique indexes declared `NULLS NOT DISTINCT` and one index with an
`INCLUDE` column (discourse), columns typed by an extension that is not contrib
(discourse's three `halfvec` columns, which need pgvector), and
foreign keys that cross a schema boundary: 20 of adventureworks' 90 span its
five schemas, 12 of mimiciv's 51 point from `mimiciv_icu` into `mimiciv_hosp`,
and every one of gitlab's partitions is attached across one.

## Load-time adjustments

Some upstream dumps cannot be piped into `psql` as they are. The loader targets
strip only what is irrelevant to a schema round trip:

- **adventureworks**: `\copy` lines are dropped (the data lives in CSVs that are
  not fetched), along with the inline `Production.ProductReview` INSERT, whose
  foreign key targets would be missing.
- **camunda**: the schema ships as one file per engine component and none of
  them create a schema, so `camunda` is created up front and the files are
  concatenated in dependency order (process engine, history, identity, then the
  case and decision engines with their history).
- **clubdata**: the dump creates its own database and reconnects to it, which
  cannot be done mid-pipe. Those two lines are dropped; the rest creates the
  `cd` schema itself.
- **demodb**: `btree_gist` is created first for the `bookings.routes` exclusion
  constraint, and the `\copy` lines are dropped.
- **discourse**: the dump belongs in a schema of its own like the group below,
  but it is `pg_dump` output that empties `search_path` and qualifies every
  object with `public`, so neither `PGOPTIONS` nor hive's one-line rewrite
  reaches it. The line that empties `search_path` is dropped and the `public.`
  qualifier is stripped, which leaves every name unqualified for `search_path`
  to place. The four `CREATE EXTENSION` lines say `WITH SCHEMA public` without a
  dot, so they are untouched and the types they own still resolve from `public`,
  which stays second in the search path. The tail of the file is Rails' own
  `SET search_path` followed by the migration versions it inserts into
  `schema_migrations`, which is data, so everything from that line on is
  dropped.
- **hive**: the dump belongs in a schema of its own like the group below, but
  it is `pg_dump` output that sets `search_path` to `public` itself, which
  overrides anything `PGOPTIONS` passes in. That one line is rewritten to name
  the `hive` schema.
- **imdb**: the schema and its foreign key indexes ship as two files, so
  `schema.sql` and `fkindexes.sql` are concatenated.
- **kea**, **dolphinscheduler**, **wso2apim**: each dump drops what it is about
  to create with `IF EXISTS`, so `client_min_messages` is raised to `warning`
  for the load.
- **mediawiki**, **synapse**, **temporal**, **icingadb**, **rt**, **znuny**,
  **ranger**, **ambari**, **ovirt**, **gitlab**, **ledgersmb**, **koji**,
  **kea**, **dolphinscheduler**, **wso2apim**, **icinga_director**,
  **flowable**, **ejabberd**, **guacamole**: these dumps name no schema at
  all, so whichever schema comes first in `search_path` gets them. Each is
  loaded into a schema of its own instead of
  `public`, so that `make schema`, which puts every sample in one database, does
  not stack them on top of the other public samples (mediawiki and pagila both
  define `actor` and `category`). gitlab creates `gitlab_partitions_static` and
  `gitlab_partitions_dynamic` itself and never qualifies anything with `public`,
  so its 1,083 top-level tables follow `search_path` into `gitlab` while its
  partitions stay in the two schemas it named.
- **mimiciv**: the schema ships as three files, so `create.sql` (tables),
  `constraint.sql` (primary and foreign keys), and `index.sql` are concatenated
  in that order. Both later files drop what they create with `IF EXISTS` first,
  so NOTICEs are quieted.
- **musicbrainz**: the schema ships as one file per object kind and none of them
  create the schema, so `musicbrainz` is created up front and the files are
  concatenated in dependency order (extensions and collation, search
  configuration, types, tables, functions, then keys, indexes, constraints, and
  views).
- **ranger**: the dump drops every object it is about to create with
  `IF EXISTS` and commits outside a transaction, which floods a fresh database
  with a few hundred NOTICEs and warnings, so `client_min_messages` is raised to
  `error` for the load.
- **znuny**: the schema ships as two files, so `schema.postgresql.sql` (tables
  and indexes) and `schema-post.postgresql.sql` (foreign keys, which need every
  table to exist) are concatenated in that order.

None of these touch table, column, index, constraint, view, or type
definitions, so the round trip still covers the full schema.

## Check-time flags

The last field of a `SAMPLES` record holds extra flags for the `pista plan`
step. Only gitlab uses it, with `--assume-validated`.

gitlab's schema has 68 constraints that are `NOT VALID`. `pista dump` writes
CHECK constraints inline in `CREATE TABLE`, and PostgreSQL accepts `NOT VALID`
only on `ALTER TABLE ... ADD CONSTRAINT`, so an inline constraint necessarily
reads back as validated and the plan asks to validate 39 constraints that are
already in the database. `--assume-validated` ignores validation state on both
sides, which is what the check is after here: whether the tables, columns,
indexes, constraints, and partitions round-trip. No other sample has an
unvalidated constraint, so no other sample needs the flag.

## Adding a sample

1. Add a line to `SAMPLES` in `sample-db.mk`: name, loader target, loader
   variables, and the schemas for `pista -n`.
2. Reuse a loader target if the source fits one (`sample-db` for the Neon
   collection, `sample-db-tar` for a tarball, `sample-db-url` for a plain SQL
   URL, `sample-db-url-schema` for a plain SQL URL that names no schema and
   should not land in `public`). Otherwise add a target, and comment why the
   plain pipe does not work.
3. If the source is on GitHub, put a commit SHA in the URL, not a branch name.
4. Run `make test-samples` and confirm the new sample reports `PASS`.

A `DRIFT` result is the interesting outcome: it means pistachio reads or writes
that schema incorrectly. Fix the catalog reader, the parser, or the diff before
adding the sample, rather than trimming the schema to make it pass.
