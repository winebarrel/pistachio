# Sample database tests

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

The runner exports `PISTA_MANAGE_ROUTINE=1` and `PISTA_MANAGE_STORAGE_PARAM=1`,
so functions, procedures and a table's storage parameters are part of the round
trip even though they are opt-in on the command line. They are environment
variables rather than per-sample flags because they have to reach both the dump
and the plan, and the manifest's flags column reaches only the plan.

The server also needs pgvector for the discourse sample's `halfvec` columns and
PostGIS for the osm and inaturalist samples' `geometry` columns. The official
postgres image ships neither. compose.yaml installs
`postgresql-<major>-pgvector` and `postgresql-<major>-postgis-3` from PGDG when
a container starts, and the samples CI job installs the same packages into its
service container, so both keep the official image and add the extensions to it.
The runner checks for them up front and says so if one is missing; recreate the
container with `docker compose down && docker compose up -d`.

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
2. Runs the sample's loader target to download and load the schema. Every
   loader pipes into `psql -v ON_ERROR_STOP=1`, so a statement that fails
   stops the load and the sample reports `FAIL (load)`. Without it psql
   prints the error, carries on, and exits 0, and the check then runs on a
   schema quietly missing whatever the failed statement was going to create;
   `dump` and `plan` agree about what is there, so the sample passes and the
   loss goes unnoticed.
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
| dotcms | dotcms | [dotCMS/core](https://github.com/dotCMS/core) |
| osm | osm | [openstreetmap/openstreetmap-website](https://github.com/openstreetmap/openstreetmap-website) |
| chado | chado, genetic_code, so, frange | [GMOD/Chado](https://github.com/GMOD/Chado) |
| wso2is | wso2is | [wso2/carbon-identity-framework](https://github.com/wso2/carbon-identity-framework) |
| nightingale | nightingale | [ccfos/nightingale](https://github.com/ccfos/nightingale) |
| danbooru | danbooru | [danbooru/danbooru](https://github.com/danbooru/danbooru) |
| openolat | openolat | [OpenOLAT/OpenOLAT](https://github.com/OpenOLAT/OpenOLAT) |
| inaturalist | inaturalist | [inaturalist/inaturalist](https://github.com/inaturalist/inaturalist) |
| joomla | joomla | [joomla/joomla-cms](https://github.com/joomla/joomla-cms) |
| harbor | harbor | [goharbor/harbor](https://github.com/goharbor/harbor) |

## Coverage

Object counts of the loaded schemas, as of 2026-08-08 on PostgreSQL 15.18 (16.13
for icingadb, rt, znuny, gitlab, hive, ranger, ambari, ovirt, and chado, the
last counted 2026-08-24; the Sequences column was counted on 15.18 throughout,
Triggers, added 2026-08-24, and Routines, added 2026-08-25, on 15.18 for every
sample; wso2is, nightingale, and danbooru were counted 2026-08-29 on 15.17;
openolat and inaturalist 2026-08-30 on 16.13; joomla and harbor 2026-09-01 on
16.13). "Constraints" excludes foreign
keys; "Types" counts enums and domains; "Sequences" counts standalone sequences
only, since pistachio manages the sequence behind a serial or identity column as
an attribute of that column rather than as an object of its own. Counting those
too would add 2,206 more, 886 of them gitlab's and 210 chado's. "Triggers"
excludes the internal triggers a foreign key installs and the clones PostgreSQL
puts on each partition of a partitioned table's trigger, the same as what
pistachio reads and dump writes. "Routines" counts what `--manage-routine`
reads, so the aggregates, window functions, and `BEGIN ATOMIC` bodies pistachio
leaves to `-- pista:execute` are out of it. All counts are limited to the
schemas the sample is checked with, and exclude what an extension owns: the two
views `pg_stat_statements` adds to sourcegraph's schema are not sourcegraph's
schema and pistachio does not read them either.

| Sample | Tables | Columns | Indexes | FKs | Constraints | Views | Types | Sequences | Triggers | Routines |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| chinook | 11 | 64 | 21 | 11 | 11 | 0 | 0 | 0 | 0 | 0 |
| dvdrental | 15 | 86 | 32 | 18 | 16 | 7 | 2 | 13 | 15 | 9 |
| happiness_index | 1 | 9 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 |
| lego | 8 | 28 | 6 | 0 | 6 | 0 | 0 | 0 | 0 | 0 |
| netflix | 1 | 12 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 |
| pagila | 22 | 129 | 55 | 36 | 23 | 8 | 3 | 13 | 15 | 9 |
| periodic_table | 1 | 28 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 |
| titanic | 1 | 21 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 0 |
| world | 3 | 24 | 3 | 2 | 4 | 0 | 0 | 0 | 0 | 0 |
| usda | 10 | 67 | 15 | 10 | 9 | 0 | 0 | 0 | 0 | 0 |
| dellstore2 | 8 | 52 | 11 | 3 | 5 | 0 | 0 | 0 | 0 | 1 |
| french_towns | 3 | 14 | 9 | 2 | 9 | 0 | 0 | 0 | 0 | 0 |
| iso_3166 | 2 | 7 | 2 | 1 | 2 | 0 | 0 | 0 | 0 | 0 |
| northwind | 14 | 92 | 14 | 13 | 14 | 0 | 0 | 0 | 0 | 0 |
| employees | 6 | 24 | 9 | 6 | 6 | 0 | 1 | 0 | 0 | 0 |
| mimiciv | 31 | 342 | 67 | 51 | 24 | 0 | 0 | 0 | 0 | 0 |
| mediawiki | 64 | 389 | 192 | 0 | 60 | 0 | 1 | 0 | 0 | 0 |
| synapse | 134 | 624 | 236 | 14 | 86 | 0 | 0 | 10 | 1 | 1 |
| temporal | 37 | 217 | 44 | 0 | 40 | 0 | 0 | 0 | 0 | 0 |
| icingadb | 66 | 634 | 179 | 7 | 74 | 0 | 13 | 0 | 0 | 2 |
| rt | 38 | 407 | 88 | 1 | 38 | 0 | 0 | 32 | 0 | 0 |
| sourcegraph | 180 | 1,715 | 453 | 362 | 273 | 18 | 8 | 1 | 29 | 37 |
| imdb | 21 | 108 | 44 | 0 | 21 | 0 | 0 | 0 | 0 | 0 |
| adventureworks | 68 | 456 | 71 | 90 | 157 | 21 | 0 | 0 | 0 | 0 |
| clubdata | 3 | 19 | 10 | 3 | 3 | 0 | 0 | 0 | 0 | 0 |
| demodb | 9 | 45 | 15 | 8 | 21 | 3 | 0 | 0 | 0 | 0 |
| musicbrainz | 374 | 2,469 | 907 | 770 | 1,032 | 10 | 7 | 0 | 0 | 130 |
| znuny | 126 | 1,103 | 326 | 286 | 190 | 0 | 0 | 0 | 0 | 0 |
| hive | 84 | 546 | 141 | 55 | 91 | 0 | 0 | 0 | 0 | 0 |
| ranger | 85 | 889 | 305 | 213 | 130 | 1 | 0 | 84 | 0 | 2 |
| ambari | 113 | 693 | 172 | 124 | 140 | 0 | 0 | 0 | 0 | 0 |
| ovirt | 152 | 1,386 | 377 | 165 | 167 | 1 | 0 | 9 | 0 | 0 |
| gitlab | 1,422 | 14,293 | 6,353 | 2,325 | 3,949 | 15 | 0 | 8 | 388 | 337 |
| ledgersmb | 158 | 950 | 263 | 247 | 265 | 1 | 0 | 2 | 11 | 7 |
| koji | 68 | 415 | 150 | 183 | 159 | 0 | 0 | 0 | 0 | 2 |
| kea | 64 | 475 | 172 | 73 | 71 | 0 | 0 | 0 | 81 | 130 |
| dolphinscheduler | 64 | 623 | 149 | 0 | 80 | 0 | 0 | 30 | 0 | 0 |
| camunda | 49 | 681 | 281 | 42 | 56 | 0 | 0 | 0 | 0 | 0 |
| wso2apim | 247 | 1,495 | 421 | 168 | 353 | 0 | 0 | 104 | 1 | 1 |
| discourse | 354 | 3,173 | 1,114 | 23 | 336 | 1 | 2 | 0 | 4 | 0 |
| icinga_director | 121 | 872 | 381 | 171 | 236 | 0 | 21 | 0 | 0 | 1 |
| flowable | 62 | 844 | 222 | 60 | 66 | 0 | 0 | 0 | 0 | 0 |
| ejabberd | 42 | 258 | 78 | 5 | 15 | 0 | 0 | 0 | 0 | 0 |
| guacamole | 23 | 104 | 61 | 30 | 29 | 0 | 5 | 0 | 0 | 0 |
| dotcms | 167 | 1,373 | 346 | 113 | 190 | 0 | 0 | 22 | 10 | 17 |
| osm | 57 | 391 | 155 | 71 | 55 | 0 | 8 | 0 | 0 | 2 |
| chado | 213 | 1,017 | 837 | 472 | 399 | 1,864 | 0 | 1 | 0 | 94 |
| wso2is | 172 | 1,108 | 362 | 128 | 271 | 0 | 0 | 92 | 0 | 0 |
| nightingale | 47 | 514 | 116 | 0 | 59 | 0 | 0 | 0 | 0 | 0 |
| danbooru | 66 | 641 | 456 | 93 | 65 | 2 | 0 | 0 | 0 | 3 |
| openolat | 382 | 4,378 | 1,239 | 632 | 423 | 8 | 0 | 0 | 0 | 0 |
| inaturalist | 186 | 1,673 | 580 | 1 | 159 | 0 | 0 | 0 | 0 | 4 |
| joomla | 76 | 830 | 287 | 0 | 84 | 0 | 0 | 0 | 0 | 1 |
| harbor | 48 | 390 | 119 | 13 | 90 | 0 | 0 | 0 | 10 | 1 |
| **Total** | **5,779** | **49,197** | **17,950** | **7,101** | **10,066** | **1,960** | **71** | **421** | **565** | **791** |

The 54 dumps come to about 163,000 lines of SQL. chado is 43,700 of them, the
longest dump of any sample, and gitlab 34,700. gitlab is still about 40 percent
of the constraints, more than a third of the indexes, and roughly a third of the
columns and foreign keys, over a quarter of the tables; musicbrainz, openolat,
discourse, and wso2apim are the largest of what remains, and chado is nearly all
of the views. gitlab is also why `clean-schema` drops tables a batch at a time
rather than cascading through `DROP SCHEMA`: a single statement takes locks on
every object it reaches, and gitlab's 1,422 tables and their indexes run the
server out of lock table space at the default `max_locks_per_transaction`. It is
why `reset-db` resets only `public` between samples, too.

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
installed at once), two gist indexes that name an `inet_ops` operator class and
one over four columns, which needs `btree_gist` (osm), materialized
views (adventureworks, pagila), tsvector
columns (dvdrental, pagila), a non-default
collation (musicbrainz), composite types (ovirt declares 10 of them, more than
any other sample, and sourcegraph and chado 2 each), standalone sequences
rather than serial columns (ranger, whose 85 tables come with 84 of them,
wso2apim, which mixes 104 of them in with serial columns, and wso2is, which
declares 92 for its 172 tables and wires 87 of them into a column DEFAULT), a
schema written entirely in quoted mixed-case identifiers, so every name is
case-sensitive (hive's 84 tables, where chinook has 11), index-heavy schemas
(danbooru's 456 indexes over 66 tables are seven to a table, denser than any
other sample, 55 of them gin, 29 of those over an expression and 17 naming
`gin_trgm_ops`, and 49 partial; mediawiki's 192 over 64, only one of them
partial and none over an expression), a schema whose size is all width and no
variety, every one of its 1,239 indexes btree and every one of its 632 foreign
keys left at NO ACTION (openolat, whose 382 tables are more than any sample but
gitlab), partitioned tables at scale (gitlab
declares 100 of them and attaches 2,054 partitions, all of which live in
schemas of their own), table
inheritance (ledgersmb attaches 21 children with INHERITS, the only sample that
does), four unique indexes declared `NULLS NOT DISTINCT` and one index with an
`INCLUDE` column (discourse), views at scale (chado's 1,864 are more than
twenty times every other sample put together, and 1,832 of them are the
Sequence Ontology views in its `so` schema, each selecting from tables in
`chado`), gist indexes over a function the schema defines itself (chado's three
name `boxrange`, one of them partial and declared from another schema), columns
typed by an extension that is not contrib (discourse's three `halfvec` columns,
which need pgvector, and the `geometry` columns that need PostGIS: osm's one
`geometry(Polygon,4326)` and inaturalist's 26, 8 of them carrying a modifier of
their own and 8 gist indexes over them, and those modifiers are the only ones
any sample reports in mixed case), and
foreign keys that cross a schema boundary: 20 of adventureworks' 90 span its
five schemas, 12 of mimiciv's 51 point from `mimiciv_icu` into `mimiciv_hosp`,
4 of chado's 472 point from `frange` into `chado`, and every one of gitlab's
partitions is attached across one; and triggers
(gitlab's 388, more than any other sample, one of them held in `ENABLE ALWAYS`
state; kea's 81 outnumber its 64 tables; ledgersmb's 11 and dotcms's 10 come
next).

Routines are concentrated the same way. Twenty-one of the 54 samples declare
one at all, and gitlab's 337, kea's and musicbrainz's 130 each, and chado's 94
are 691 of the 791. Two thirds of them, 524, return `trigger`, though not
every one of those has a trigger to call it: musicbrainz's 89 do not, since its
loader concatenates a file list that leaves triggers out. 714 are written in
plpgsql and 77 in sql. sourcegraph declares the only procedure any sample has,
and inaturalist the only aggregate, which `--manage-routine` does not read and
so is in neither count. Only chado and kea overload a name, 11 of them and 3,
though danbooru's three, all sql, include a `lower(text[])` that shadows a
built-in.
Only sourcegraph, ledgersmb, and gitlab comment a routine, seven between them.

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
- **chado**: the file names no schema for the objects it creates, but four
  times partway through it sets `search_path` itself with `public` in it, which
  overrides anything `PGOPTIONS` passes in. The `public` in those four lines is
  rewritten to `chado`, the way hive's one line is. The lines that name
  `genetic_code`, `so`, and `frange` keep them: the file creates those three
  schemas itself, so the sample is checked with all four. Its tables are all
  `bigserial`, so `client_min_messages` is raised to `warning` to quiet the
  implicit-sequence NOTICEs. Nine of its SQL functions are dropped, the six
  written against the `@` box operator that PostgreSQL 14 removed and the three
  that call one of those six; they error out on every version in the CI matrix.
  The 94 that load are part of the round trip like any other object. The
  `create_point` calls in the bodies of `boxrange` and `boxquery` are qualified
  with `chado.`, because PostgreSQL 17 runs `CREATE INDEX` with `search_path`
  set to `pg_catalog, pg_temp`: the three gist indexes over `boxrange` inline
  it, which re-resolves an unqualified `create_point` under that `search_path`
  and does not find it. With the calls qualified, every table, index,
  constraint, and view loads on all four versions of the CI matrix, the three
  gist indexes among them.
- **clubdata**: the dump creates its own database and reconnects to it, which
  cannot be done mid-pipe. Those two lines are dropped; the rest creates the
  `cd` schema itself.
- **demodb**: `btree_gist` is created first for the `bookings.routes` exclusion
  constraint, and the `\copy` lines are dropped.
- **discourse**, **osm**, **danbooru**, **inaturalist**: all four ship their
  schema as Rails' `db/structure.sql`, which belongs in a schema of its own like
  the group below but is `pg_dump` output that empties `search_path` and
  qualifies every object with `public`, so neither `PGOPTIONS` nor hive's
  one-line rewrite reaches it. The line that empties `search_path` is dropped
  and the `public.` qualifier is stripped, which leaves every name unqualified
  for `search_path` to place. The `CREATE EXTENSION` lines say `WITH SCHEMA
  public` without a dot, so they are untouched and the types they own still
  resolve from `public`, which stays second in the search path. The tail of the
  file is Rails' own `SET search_path` followed by the migration versions it
  inserts into `schema_migrations`, which is data, so everything from that line
  on is dropped. danbooru installs five extensions of its own, `btree_gin`,
  `fuzzystrmatch`, `pg_trgm`, `pgcrypto`, and `pgstattuple`, but all five are
  contrib and the official image already has them, so it needs nothing installed
  the way discourse and osm do. inaturalist needs PostGIS, as osm does, and
  `uuid-ossp`, which is contrib and which 16 of its columns default through.
- **dvdrental**: the dump was taken by a `pg_dump` new enough to set
  `transaction_timeout` in its preamble, which 15 and 16 do not have, so that
  one line is dropped. It sets nothing the schema depends on.
- **harbor**: the schema ships as one file per release, each a delta meant to
  be replayed by golang-migrate, which tracks what it has applied in a
  `schema_migrations` table of its own. One delta `ALTER TABLE`s that table
  directly, so a stand-in `schema_migrations` is created before the deltas run
  and dropped once they have; it is golang-migrate's bookkeeping, not part of
  Harbor's schema. A few deltas also omit their file's closing `;`, which
  merges the next file's opening statement into it once concatenated, so
  every file gets one appended regardless of whether it already ends in one.
- **hive**: the dump belongs in a schema of its own like the group below, but
  it is `pg_dump` output that sets `search_path` to `public` itself, which
  overrides anything `PGOPTIONS` passes in. That one line is rewritten to name
  the `hive` schema.
- **imdb**: the schema and its foreign key indexes ship as two files, so
  `schema.sql` and `fkindexes.sql` are concatenated.
- **joomla**: the schema ships as three files that must load in order --
  `base.sql`, `extensions.sql`, and `supports.sql`, the last of which
  references content types `extensions.sql` creates -- so they are
  concatenated. None of the three create a schema or set `search_path`
  themselves, and every table name carries the literal `#__` prefix Joomla
  substitutes at install time; quoted, it is just an ordinary identifier and
  needs no rewriting.
- **kea**, **dolphinscheduler**, **wso2apim**, **wso2is**: each dump drops what
  it is about to create with `IF EXISTS`, so `client_min_messages` is raised to
  `warning` for the load. wso2is is also where five index names run past the 63
  character identifier limit, and the server says so as it truncates them.
- **mediawiki**, **synapse**, **temporal**, **icingadb**, **rt**, **znuny**,
  **ranger**, **ambari**, **ovirt**, **gitlab**, **ledgersmb**, **koji**,
  **kea**, **dolphinscheduler**, **wso2apim**, **icinga_director**,
  **flowable**, **ejabberd**, **guacamole**, **dotcms**, **wso2is**,
  **nightingale**, **openolat**: these dumps name no schema at all, so whichever
  schema comes first in `search_path` gets them. Each is loaded into a schema of
  its own instead of `public`, so that `make schema`, which puts every sample in
  one database, does not stack them on top of the other public samples
  (mediawiki and pagila both define `actor` and `category`). gitlab creates
  `gitlab_partitions_static` and `gitlab_partitions_dynamic` itself and never
  qualifies anything with `public`, so its 1,083 top-level tables follow
  `search_path` into `gitlab` while its partitions stay in the two schemas it
  named.
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

None of these touch table, column, index, constraint, view, type, or routine
definitions, chado's nine unloadable functions aside, so the round trip still
covers the full schema.

## Check-time flags

The last field of a `SAMPLES` record holds extra flags for the `pista plan`
step. No sample uses it today. gitlab used `--assume-validated` while `pista
dump` wrote every CHECK constraint inline in `CREATE TABLE`, where `NOT VALID`
cannot be spelled; dump now writes such a check as its own `ALTER TABLE`, and
gitlab's 68 `NOT VALID` constraints round-trip without the flag.

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
