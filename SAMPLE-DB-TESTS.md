# Sample database tests

pistachio is checked against real-world PostgreSQL schemas, not only against
the hand-written fixtures in `testdata/`. Each sample database is downloaded
from its upstream source and loaded into an isolated database. It is then
round-tripped through `pista dump` and `pista plan`.

## Running

```bash
make test-samples
```

The target runs `test/samples/run.sh`. That script builds `pista` and drives
the check. It needs a running PostgreSQL instance (`PGHOST=localhost` and
`PGUSER=postgres`, which the Makefile exports). It also needs `psql`, `curl`,
and network access to the upstream hosts. The uyuni sample needs GNU `make` and
`python3` as well. Its schema is a source tree rather than a file, and the
loader runs the build that turns the tree into a file. The same target runs in
CI as the `samples` job.

To check some of the samples rather than all of them, name them in `SAMPLE`,
separated by commas:

```bash
make test-samples SAMPLE=lemmy,cratesio
```

The named samples run in manifest order, whatever order they are named in. A
name that the manifest does not hold stops the run. The pgvector and PostGIS
checks below apply only when a named sample needs the extension.

The runner exports `PISTA_MANAGE_ROUTINE=1` and `PISTA_MANAGE_STORAGE_PARAM=1`.
So functions, procedures and a table's storage parameters are part of the round
trip, even though they are opt-in on the command line. These settings are
environment variables rather than per-sample flags because they have to reach
both the dump and the plan. The manifest's flags column reaches only the plan.

The server also needs pgvector for the discourse sample's `halfvec` columns and
for citizenlab's `vector` column. It needs PostGIS for the `geometry` columns
of the osm, inaturalist, and dhis2 samples and for citizenlab's `geography`
columns. The official postgres image ships neither extension. compose.yaml
installs `postgresql-<major>-pgvector` and `postgresql-<major>-postgis-3` from
PGDG when a container starts. The samples CI job installs the same packages
into its service container. So both keep the official image and add the
extensions to it. The runner checks for the extensions up front and reports a
missing one. In that case, recreate the container with
`docker compose down && docker compose up -d`.

To load every sample into one database for manual inspection, instead of
checking them one at a time, use `make schema`.

## What the check does

The runner starts with `make clean-schema`. That target drops every extension
and then every user schema. So a schema or an extension that an earlier run
left behind cannot make a load fail on objects that already exist. The
extensions go first because a table that an extension owns cannot be dropped
while the extension is there. PostGIS's `spatial_ref_sys` is one such table.
Then, for each sample, the runner:

1. Runs `make reset-db`. That target drops and recreates `public` and drops
   every extension. The samples that load into `public` are the only ones that
   can collide with each other. Every other sample owns a schema of its own and
   is checked with `pista -n`. So what such a sample leaves behind is invisible
   to the next sample. Extensions are the exception. They are visible whichever
   schema they sit in. A dump that says `CREATE EXTENSION IF NOT EXISTS` does
   nothing when an earlier sample already installed that extension somewhere
   else. That leaves the extension's types and operator classes unresolvable.
2. Runs the sample's loader target to download and load the schema. Every
   loader pipes into `psql -v ON_ERROR_STOP=1`. So a statement that fails
   stops the load, and the sample reports `FAIL (load)`. Without that option,
   psql prints the error, carries on, and exits 0. The check then runs on a
   schema that quietly lacks whatever the failed statement was going to
   create. `dump` and `plan` agree about what is there. So the sample passes,
   and the loss goes unnoticed.
3. Runs `pista dump -n <schemas>` to capture pistachio's model of the loaded
   schema as SQL.
4. Runs `pista plan -n <schemas> <dump>` and requires the output to be
   "No changes".

The two commands exercise opposite directions of the same model. `dump` goes
catalog reader -> model -> SQL. `plan` goes parser -> model -> diff against the
catalog. If the dump plans to anything other than "No changes", the catalog
reader and the parser disagree about the schema. That disagreement is a bug,
whichever side is wrong.

Each sample reports `PASS`, `DRIFT` (the plan was not empty), or a `FAIL` with
the failing stage (`load`, `dump`, or `plan`). Failure output is printed
indented under the sample name. The script exits non-zero if any sample failed.

## Samples

The sample list lives in the `SAMPLES` variable in `sample-db.mk`, which the
Makefile includes. The variable holds one record per line: name, loader target,
loader variables, the schemas that are passed to `pista -n` (blank means
`public`), and any extra `pista plan` flags (only gitlab needs one).
`make print-samples` prints the list for shell consumers. So `sample-db.mk`
stays the single source of the list.

Every GitHub source is fetched at a pinned commit rather than at a branch. So
an upstream schema change cannot turn CI red on its own, and the object counts
below stay accurate. To move a sample to a newer upstream schema, resolve the
branch with `git ls-remote https://github.com/<owner>/<repo> <branch>`. Then
replace the SHA in `sample-db.mk` and re-run `make test-samples`. omop is
pinned to a release tag rather than to a branch tip, because the files at the
tip do not load. Its loader says why.

| Sample | Schemas | Source |
|---|---|---|
| boundary | boundary | [hashicorp/boundary](https://github.com/hashicorp/boundary) |
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
| bigbluebutton | bigbluebutton | [bigbluebutton/bigbluebutton](https://github.com/bigbluebutton/bigbluebutton) |
| listmonk | listmonk | [knadh/listmonk](https://github.com/knadh/listmonk) |
| dhis2 | dhis2 | [dhis2/dhis2-core](https://github.com/dhis2/dhis2-core) |
| coder | coder | [coder/coder](https://github.com/coder/coder) |
| hatchet | hatchet | [hatchet-dev/hatchet](https://github.com/hatchet-dev/hatchet) |
| thingsboard | thingsboard | [thingsboard/thingsboard](https://github.com/thingsboard/thingsboard) |
| glific | glific | [glific/glific](https://github.com/glific/glific) |
| lago | lago | [getlago/lago-api](https://github.com/getlago/lago-api) |
| calcom | calcom | [calcom/cal.diy](https://github.com/calcom/cal.diy) |
| triggerdev | triggerdev | [triggerdotdev/trigger.dev](https://github.com/triggerdotdev/trigger.dev) |
| mattermost | mattermost | [mattermost/mattermost](https://github.com/mattermost/mattermost) |
| lemmy | lemmy, r, utils | [LemmyNet/lemmy](https://github.com/LemmyNet/lemmy) |
| windmill | windmill | [windmill-labs/windmill](https://github.com/windmill-labs/windmill) |
| plausible | plausible | [plausible/analytics](https://github.com/plausible/analytics) |
| feedbin | feedbin | [feedbin/feedbin](https://github.com/feedbin/feedbin) |
| citizenlab | citizenlab | [CitizenLabDotCo/citizenlab](https://github.com/CitizenLabDotCo/citizenlab) |
| dokploy | dokploy | [Dokploy/dokploy](https://github.com/Dokploy/dokploy) |
| hyperswitch | hyperswitch | [juspay/hyperswitch](https://github.com/juspay/hyperswitch) |
| documenso | documenso | [documenso/documenso](https://github.com/documenso/documenso) |
| langfuse | langfuse | [langfuse/langfuse](https://github.com/langfuse/langfuse) |
| icinga_ido | icinga_ido | [Icinga/icinga2](https://github.com/Icinga/icinga2) |
| openfire | openfire | [igniterealtime/Openfire](https://github.com/igniterealtime/Openfire) |
| bareos | bareos | [bareos/bareos](https://github.com/bareos/bareos) |
| opencms | opencms | [alkacon/opencms-core](https://github.com/alkacon/opencms-core) |
| marquez | marquez | [MarquezProject/marquez](https://github.com/MarquezProject/marquez) |
| penpot | penpot | [penpot/penpot](https://github.com/penpot/penpot) |
| dcm4chee | dcm4chee | [dcm4che/dcm4chee-arc-light](https://github.com/dcm4che/dcm4chee-arc-light) |
| kamailio | kamailio | [kamailio/kamailio](https://github.com/kamailio/kamailio) |
| alfresco | alfresco | [Alfresco/alfresco-community-repo](https://github.com/Alfresco/alfresco-community-repo) |
| roundcube | roundcube | [roundcube/roundcubemail](https://github.com/roundcube/roundcubemail) |
| shenyu | shenyu | [apache/shenyu](https://github.com/apache/shenyu) |
| nacos | nacos | [alibaba/nacos](https://github.com/alibaba/nacos) |
| openreplay | openreplay, events, events_common, events_ios, spots | [openreplay/openreplay](https://github.com/openreplay/openreplay) |
| logto | logto | [logto-io/logto](https://github.com/logto-io/logto) |
| omero | omero | [ome/openmicroscopy](https://github.com/ome/openmicroscopy) |
| concourse | concourse | [concourse/concourse](https://github.com/concourse/concourse) |
| affine | affine | [toeverything/AFFiNE](https://github.com/toeverything/AFFiNE) |
| teable | teable | [teableio/teable](https://github.com/teableio/teable) |
| uyuni | uyuni, access, rpm, deb, rhn_cache, rhn_channel, rhn_config, rhn_config_channel, rhn_entitlements, rhn_exception, rhn_org, rhn_server, rhn_user | [uyuni-project/uyuni](https://github.com/uyuni-project/uyuni) |
| lobehub | lobehub | [lobehub/lobehub](https://github.com/lobehub/lobehub) |
| hexpm | hexpm | [hexpm/hexpm](https://github.com/hexpm/hexpm) |
| omop | omop | [OHDSI/CommonDataModel](https://github.com/OHDSI/CommonDataModel) |
| zed | zed | [zed-industries/zed](https://github.com/zed-industries/zed) |
| gravitino | gravitino | [apache/gravitino](https://github.com/apache/gravitino) |
| formbricks | formbricks | [formbricks/formbricks](https://github.com/formbricks/formbricks) |
| hoppscotch | hoppscotch | [hoppscotch/hoppscotch](https://github.com/hoppscotch/hoppscotch) |
| streampark | streampark | [apache/streampark](https://github.com/apache/streampark) |
| vaultwarden | vaultwarden | [dani-garcia/vaultwarden](https://github.com/dani-garcia/vaultwarden) |
| authelia | authelia | [authelia/authelia](https://github.com/authelia/authelia) |
| hydra | hydra | [ory/hydra](https://github.com/ory/hydra) |
| bonita | bonita | [bonitasoft/bonita-engine](https://github.com/bonitasoft/bonita-engine) |
| ghostfolio | ghostfolio | [ghostfolio/ghostfolio](https://github.com/ghostfolio/ghostfolio) |
| typebot | typebot | [baptisteArno/typebot.io](https://github.com/baptisteArno/typebot.io) |
| cratesio | cratesio | [rust-lang/crates.io](https://github.com/rust-lang/crates.io) |

## Coverage

This section gives the object counts of the loaded schemas.

### How the counts were taken

The counts were taken on 2026-08-08 on PostgreSQL 15.18, with these
exceptions:

- icingadb, rt, znuny, gitlab, hive, ranger, ambari, ovirt, and chado were
  counted on 16.13. chado was counted on 2026-08-24.
- wso2is, nightingale, and danbooru were counted on 2026-08-29 on 15.17.
- openolat and inaturalist were counted on 2026-08-30 on 16.13.
- joomla and harbor were counted on 2026-09-01 on 16.13.
- bigbluebutton and listmonk were counted on 2026-09-11 on 16.13.
- dhis2 was counted on 2026-09-15 on 15.18.
- coder, boundary, hatchet, thingsboard, glific, lago, calcom, and triggerdev
  were counted on 2026-09-17 on 15.18.
- mattermost, lemmy, windmill, plausible, feedbin, and citizenlab were counted
  on 2026-09-17 on 16.13. dokploy, hyperswitch, documenso, langfuse,
  icinga_ido, openfire, bareos, opencms, and marquez were counted on 2026-09-18
  on 16.13. penpot was counted on 2026-09-20 on 16.13. dcm4chee, kamailio,
  alfresco, roundcube, shenyu, and nacos were counted on 2026-09-20 on 16.13.
- openreplay and logto were counted on 2026-09-20 on 16.13. omero, concourse,
  affine, and teable were counted on 2026-09-21 on 15.18. uyuni, lobehub,
  hexpm, omop, zed, gravitino, formbricks, hoppscotch, and streampark were
  counted on 2026-09-21 on 16.13. vaultwarden, authelia, hydra, bonita,
  ghostfolio, and typebot were counted on 2026-09-22 on 16.13.
- cratesio was counted on 2026-09-25 on 16.13.
- The Sequences column was counted on 15.18 throughout. The Triggers column was
  added on 2026-08-24 and the Routines column on 2026-08-25. Both were counted
  on 15.18 for every sample.

The columns hold the following:

- **Constraints** excludes foreign keys.
- **Types** counts enums and domains.
- **Sequences** counts standalone sequences only. Pistachio manages the
  sequence behind a serial or identity column as an attribute of that column,
  not as an object of its own. Counting those sequences too would add 2,319
  more. 886 of them are gitlab's, 210 are chado's, and 31 are hexpm's. hexpm
  declares no standalone sequence at all. zed, with 17 such sequences, and
  gravitino, with 1, declare none either. authelia's 25 and hydra's 2 are the
  same shape. authelia has one per table except for its unkeyed table.
- **Triggers** excludes the internal triggers that a foreign key installs. It
  also excludes the clones that PostgreSQL puts on each partition of a
  partitioned table's trigger. This matches what pistachio reads and what dump
  writes.
- **Routines** counts what `--manage-routine` reads. So the aggregates and
  window functions that pistachio leaves to `-- pista:execute` are not
  counted. lemmy is the only sample with a SQL-standard body. 6 of its 80
  functions have one.
- **Policies** are not a column. windmill declares 366 of them and logto
  declares 153. No other sample turns row-level security on at all.

All counts are limited to the schemas that the sample is checked with. They
exclude what an extension owns. For example, `pg_stat_statements` adds two
views to sourcegraph's schema. Those views are not sourcegraph's schema, and
pistachio does not read them either.

| Sample | Tables | Columns | Indexes | FKs | Constraints | Views | Types | Sequences | Triggers | Routines |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| boundary | 293 | 1,530 | 562 | 387 | 685 | 62 | 36 | 0 | 741 | 225 |
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
| bigbluebutton | 54 | 532 | 147 | 65 | 51 | 86 | 0 | 0 | 25 | 26 |
| listmonk | 16 | 126 | 65 | 19 | 25 | 3 | 14 | 0 | 0 | 0 |
| dhis2 | 473 | 2,648 | 955 | 989 | 925 | 0 | 0 | 1 | 0 | 0 |
| coder | 116 | 1,173 | 292 | 153 | 217 | 11 | 62 | 1 | 30 | 30 |
| hatchet | 133 | 1,209 | 330 | 77 | 136 | 0 | 57 | 1 | 22 | 49 |
| thingsboard | 65 | 660 | 163 | 29 | 106 | 6 | 0 | 3 | 0 | 14 |
| glific | 57 | 590 | 199 | 142 | 57 | 0 | 19 | 0 | 14 | 14 |
| lago | 143 | 1,738 | 801 | 363 | 177 | 34 | 45 | 0 | 2 | 2 |
| calcom | 102 | 1,092 | 394 | 179 | 104 | 2 | 46 | 0 | 7 | 9 |
| triggerdev | 85 | 1,123 | 289 | 135 | 81 | 0 | 48 | 0 | 0 | 0 |
| mattermost | 86 | 740 | 279 | 3 | 104 | 6 | 7 | 0 | 0 | 0 |
| lemmy | 58 | 573 | 290 | 113 | 101 | 0 | 16 | 1 | 66 | 80 |
| windmill | 173 | 1,511 | 392 | 105 | 210 | 3 | 33 | 4 | 26 | 24 |
| plausible | 42 | 294 | 81 | 40 | 47 | 0 | 3 | 0 | 1 | 1 |
| feedbin | 44 | 395 | 161 | 8 | 43 | 0 | 0 | 0 | 0 | 0 |
| citizenlab | 144 | 1,304 | 498 | 169 | 155 | 26 | 0 | 0 | 2 | 4 |
| dokploy | 67 | 946 | 106 | 133 | 89 | 0 | 27 | 0 | 0 | 0 |
| hyperswitch | 50 | 1,070 | 135 | 0 | 61 | 0 | 46 | 0 | 0 | 2 |
| documenso | 51 | 490 | 138 | 63 | 51 | 0 | 30 | 0 | 0 | 4 |
| langfuse | 74 | 757 | 222 | 113 | 72 | 0 | 35 | 0 | 0 | 0 |
| icinga_ido | 61 | 791 | 234 | 0 | 94 | 0 | 0 | 0 | 0 | 3 |
| openfire | 35 | 224 | 50 | 1 | 33 | 0 | 0 | 0 | 0 | 0 |
| bareos | 28 | 259 | 39 | 0 | 26 | 0 | 0 | 0 | 0 | 2 |
| opencms | 41 | 246 | 164 | 0 | 44 | 0 | 0 | 0 | 0 | 0 |
| marquez | 30 | 199 | 83 | 46 | 38 | 4 | 0 | 0 | 2 | 2 |
| penpot | 61 | 511 | 169 | 85 | 71 | 0 | 0 | 0 | 8 | 3 |
| dcm4chee | 41 | 425 | 290 | 65 | 96 | 0 | 0 | 30 | 0 | 0 |
| kamailio | 73 | 614 | 182 | 0 | 109 | 0 | 0 | 0 | 0 | 0 |
| alfresco | 45 | 250 | 156 | 53 | 48 | 0 | 0 | 38 | 0 | 0 |
| roundcube | 18 | 99 | 35 | 14 | 21 | 0 | 0 | 8 | 0 | 0 |
| shenyu | 45 | 391 | 28 | 0 | 24 | 0 | 0 | 1 | 0 | 0 |
| nacos | 16 | 175 | 42 | 0 | 12 | 0 | 0 | 0 | 0 | 0 |
| openreplay | 62 | 520 | 268 | 84 | 65 | 0 | 19 | 0 | 3 | 5 |
| logto | 79 | 495 | 181 | 152 | 116 | 0 | 8 | 0 | 85 | 10 |
| omero | 161 | 1,488 | 811 | 696 | 408 | 50 | 12 | 130 | 130 | 57 |
| concourse | 45 | 320 | 154 | 80 | 46 | 0 | 5 | 2 | 7 | 7 |
| affine | 72 | 764 | 246 | 69 | 124 | 0 | 11 | 0 | 16 | 7 |
| teable | 62 | 703 | 197 | 23 | 62 | 0 | 8 | 0 | 0 | 2 |
| uyuni | 433 | 2,614 | 935 | 692 | 1,102 | 55 | 4 | 207 | 224 | 412 |
| lobehub | 182 | 2,474 | 972 | 550 | 237 | 0 | 0 | 1 | 0 | 0 |
| hexpm | 36 | 253 | 118 | 51 | 41 | 2 | 2 | 0 | 0 | 2 |
| omop | 39 | 432 | 98 | 176 | 28 | 0 | 0 | 0 | 0 | 0 |
| zed | 29 | 221 | 78 | 42 | 29 | 0 | 0 | 0 | 0 | 0 |
| gravitino | 20 | 186 | 52 | 0 | 39 | 0 | 0 | 0 | 0 | 0 |
| formbricks | 59 | 559 | 186 | 91 | 58 | 0 | 32 | 0 | 11 | 2 |
| hoppscotch | 23 | 171 | 49 | 22 | 25 | 0 | 4 | 0 | 2 | 1 |
| streampark | 26 | 290 | 42 | 0 | 27 | 0 | 0 | 24 | 8 | 1 |
| vaultwarden | 28 | 215 | 33 | 34 | 33 | 0 | 0 | 0 | 0 | 0 |
| authelia | 25 | 267 | 66 | 15 | 24 | 0 | 0 | 0 | 0 | 0 |
| hydra | 16 | 249 | 58 | 31 | 16 | 0 | 0 | 0 | 0 | 0 |
| bonita | 81 | 707 | 191 | 32 | 115 | 0 | 0 | 0 | 1 | 1 |
| ghostfolio | 21 | 150 | 74 | 23 | 21 | 0 | 10 | 0 | 0 | 0 |
| typebot | 31 | 245 | 53 | 31 | 23 | 0 | 5 | 0 | 0 | 0 |
| cratesio | 35 | 207 | 85 | 35 | 47 | 1 | 0 | 0 | 25 | 31 |
| **Total** | **10,164** | **87,412** | **30,868** | **13,579** | **16,865** | **2,311** | **715** | **873** | **2,023** | **1,823** |

### Size

The 109 dumps come to about 289,000 lines of SQL. chado is 43,700 of them,
which is the longest dump of any sample. gitlab is 34,700 and uyuni is 19,700.
gitlab is still about a quarter of the constraints, a fifth of the indexes, a
sixth of the foreign keys and of the columns, and a seventh of the tables.
dhis2, uyuni, openolat, musicbrainz, and discourse are the largest of what
remains. chado is nearly all of the views.

gitlab is also the reason that `clean-schema` drops tables a batch at a time
instead of cascading through `DROP SCHEMA`. A single statement takes locks on
every object that it reaches. gitlab's 1,422 tables and their indexes run the
server out of lock table space at the default `max_locks_per_transaction`.
gitlab is also the reason that `reset-db` resets only `public` between samples.

### Shapes

Beyond size, the samples bring in shapes that the hand-written fixtures do not
always reach. This list names the samples that bring in each one. Check it
before dropping a sample or changing its loader, so that a shape only one
sample covers is not lost unnoticed.

Indexes:

- gin: synapse, rt, musicbrainz, danbooru, lago, mattermost, windmill,
  langfuse, openreplay, logto, concourse, uyuni, lobehub, hexpm, zed,
  hoppscotch.
- gist: musicbrainz, osm, chado, inaturalist.
- hash: musicbrainz, langfuse, uyuni.
- brin: musicbrainz, logto.
- hnsw, pgvector's method: citizenlab, affine, lobehub.
- Partial indexes: mediawiki, synapse, musicbrainz, chado, danbooru, lago,
  mattermost, lemmy, windmill, feedbin, penpot, openreplay, logto, omero,
  uyuni, lobehub, hydra.
- Expression indexes: rt, musicbrainz, danbooru, mattermost, feedbin, langfuse,
  penpot, dcm4chee, logto, omero, uyuni, lobehub, hexpm.
- Unique indexes over an expression: rt, mattermost.
- gin over `to_tsvector`: rt, langfuse, uyuni, hexpm (with another function
  inside it).
- An operator class named: `gin_trgm_ops` (danbooru, openreplay, uyuni, hexpm,
  zed, hoppscotch), `jsonb_path_ops` (mattermost, hexpm, lobehub, concourse),
  `text_pattern_ops` (mattermost), `inet_ops` (osm).
- An operator class from an extension: qualified with the extension's schema
  (citizenlab), unqualified (affine, lobehub, zed, hoppscotch).
- The default operator class beside a named one on the same kind of index:
  hoppscotch.
- gist over a function the schema defines: chado.
- gist over several columns, needing `btree_gist`: osm.
- `NULLS NOT DISTINCT`: discourse, lago, hydra.
- `INCLUDE`: discourse, lago, hexpm, hydra (also partial).
- Storage parameters on an index: concourse.
- Bare unique indexes where a unique constraint could stand: lobehub, zed,
  formbricks, hoppscotch, authelia, typebot, ghostfolio.

Constraints and keys:

- Exclusion constraints: demodb, boundary, hatchet, lago.
- CHECK constraints added by hand to an ORM schema: affine, lobehub.
- CHECK constraints named at install time: uyuni.
- No CHECK at all: dhis2, mattermost, vaultwarden, bonita, omop, zed,
  formbricks.
- Foreign keys over more than one column: zed, formbricks, hydra (three
  columns).
- Foreign keys across schemas: adventureworks, mimiciv, chado.
- Referential actions on both sides: icinga_director, calcom, triggerdev,
  langfuse, logto, formbricks, hoppscotch, authelia; `ON UPDATE RESTRICT`:
  hydra.
- `ON DELETE` alone: glific, dokploy, openreplay, lobehub, uyuni.
- No foreign key at all: mediawiki, temporal, imdb, dolphinscheduler,
  nightingale, joomla, hyperswitch, icinga_ido, bareos, opencms, kamailio,
  shenyu, nacos, gravitino, streampark.
- Tables with no primary key: shenyu, concourse, omop, teable, typebot,
  authelia.
- Composite primary keys: vaultwarden, bonita.

Types and columns:

- Enums: dvdrental, pagila, employees, mediawiki, icingadb, and most of the
  ORM schemas. hyperswitch has the most labels, omero has labels past ASCII,
  calcom and triggerdev type columns as an array of one, and mattermost casts
  to one in a partial index predicate.
- Domains with CHECKs: icingadb (named), icinga_director, boundary, omero.
- Composite types: ovirt, sourcegraph, chado, coder, marquez (built with ROW()
  in a view), uyuni (read by an index and a generated column).
- Stored generated columns: bigbluebutton (over a function of its own), uyuni.
- Identity columns: openreplay, uyuni.
- Standalone sequences: ranger, wso2apim, wso2is, dcm4chee, alfresco,
  roundcube, omero, uyuni, streampark (non-default `START WITH` and
  `MINVALUE`).
- tsvector columns: dvdrental, pagila.
- A non-default collation: musicbrainz.
- Types from a contrib extension: sourcegraph, plausible, hexpm (`citext`),
  lemmy (`ltree`), feedbin (`hstore`).
- Types from an extension that is not contrib: discourse (`halfvec`),
  citizenlab, affine, lobehub (`vector`), osm, inaturalist, dhis2
  (`geometry`), citizenlab (`geography`). inaturalist's type modifiers are in
  mixed case.
- An `oid` column for a large object: bonita.
- Quoted mixed-case identifiers: chinook, hive, hatchet, calcom, triggerdev,
  documenso, bigbluebutton, dokploy, formbricks, hoppscotch; langfuse for its
  enum types only.
- Table and column comments: gravitino, shenyu, nacos, glific, streampark.

Tables and views:

- Unlogged tables: demodb, bigbluebutton (every table), omero.
- Partitioned tables: gitlab (partitions in other schemas), hatchet (range and
  hash, none attached), lago, triggerdev, thingsboard, windmill, penpot (hash,
  with partitioned indexes).
- Table inheritance: ledgersmb.
- Storage parameters on a table: hatchet, lobehub.
- Materialized views: adventureworks, pagila, listmonk, lago, marquez,
  mattermost and hexpm (with indexes; hexpm's are unique and order a column
  `DESC NULLS LAST`).
- Views at scale: chado, bigbluebutton (views over views), omero.
- Row-level security: windmill (policies naming roles), logto (restrictive
  policies).

Triggers:

- Constraint triggers: boundary, omero.
- A trigger in `ENABLE ALWAYS` state: gitlab.
- Statement-level triggers with transition tables, and triggers on a
  partitioned table: hatchet.
- An INSTEAD OF trigger on a view: marquez.
- `UPDATE OF` a column list: affine, formbricks (with arguments), hoppscotch.
- One trigger function per table: uyuni. One shared by every table:
  streampark.
- Trigger functions in another schema: lemmy.
- A trigger that frees a large object: bonita.

Schemas and extensions:

- Many schemas, most holding routines alone: uyuni.
- Extensions in a schema of their own: citizenlab, windmill, lemmy.
- An extension installed and then left unused: formbricks (pgvector, which
  the load still needs on the server).

## Load-time adjustments

Every loader runs `psql` with `client_min_messages` raised to `warning`, set
once in `sample-db.mk`. On a fresh database the server reports a dump that
drops what it is about to create with `IF EXISTS`, an identifier longer than
63 characters that it truncates, and an index that a constraint takes over.
None of that is about the schema under test, and the runner passes a loader's
stderr through. A sample can raise the level further, to `error`, from its
`SAMPLES` record.

A loader that installs a contrib extension into `public` follows the install
with `ALTER EXTENSION ... SET SCHEMA public`. In `make schema`, every sample
loads after one `clean-schema`, and `CREATE EXTENSION IF NOT EXISTS ... WITH
SCHEMA public` does not move an extension that an earlier sample installed
somewhere else. Without the relocation, the later sample's types, functions,
and operator classes do not resolve. PostGIS is not relocatable. dhis2
installs it without the relocation, and citizenlab, the only sample that puts
it in another schema, loads after dhis2.

Many upstream schemas also cannot be piped into `psql` as they are. Each
loader target strips or rewrites only what is irrelevant to a schema round
trip. The comment above the target in `sample-db.mk` says what it changes and
why.

## Check-time flags

The last field of a `SAMPLES` record holds extra flags for the `pista plan`
step. No sample uses it today. gitlab used `--assume-validated` while `pista
dump` wrote every CHECK constraint inline in `CREATE TABLE`, where `NOT VALID`
cannot be spelled. `pista dump` now writes such a check as its own
`ALTER TABLE`. So gitlab's 68 `NOT VALID` constraints round-trip without the
flag.

## Adding a sample

1. Add a line to `SAMPLES` in `sample-db.mk`: name, loader target, loader
   variables, and the schemas for `pista -n`.
2. Reuse a loader target if the source fits one (`sample-db` for the Neon
   collection, `sample-db-tar` for a tarball, `sample-db-url` for a plain SQL
   URL, `sample-db-url-schema` for a plain SQL URL that names no schema and
   should not land in `public`). Otherwise, add a target and comment why the
   plain pipe does not work.
3. If the source is on GitHub, put a commit SHA in the URL, not a branch name.
4. Run `make test-samples SAMPLE=<name>` and confirm the new sample reports
   `PASS`.
5. Add the sample to every entry under Shapes that it covers. If it brings in
   a shape that the list does not name, add an entry for it.
6. Leave `CHANGELOG.md` alone. A sample is test-only, and nothing about it
   reaches someone who uses pista.

A `DRIFT` result is the interesting outcome. It means that pistachio reads or
writes that schema incorrectly. Fix the catalog reader, the parser, or the diff
before adding the sample. Do not trim the schema to make it pass.
