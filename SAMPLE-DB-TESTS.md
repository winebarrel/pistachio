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
access to the upstream hosts. The uyuni sample needs GNU `make` and `python3`
as well: its schema is a source tree rather than a file, and the loader runs
the build that turns it into one. The same target runs in CI as the `samples`
job.

The runner exports `PISTA_MANAGE_ROUTINE=1` and `PISTA_MANAGE_STORAGE_PARAM=1`,
so functions, procedures and a table's storage parameters are part of the round
trip even though they are opt-in on the command line. They are environment
variables rather than per-sample flags because they have to reach both the dump
and the plan, and the manifest's flags column reaches only the plan.

The server also needs pgvector for the discourse sample's `halfvec` columns and
citizenlab's `vector` column, and PostGIS for the osm, inaturalist, and dhis2
samples' `geometry` columns and citizenlab's `geography` ones. The
official postgres image ships neither. compose.yaml installs
`postgresql-<major>-pgvector` and `postgresql-<major>-postgis-3` from PGDG when
a container starts, and the samples CI job installs the same packages into its
service container, so both keep the official image and add the extensions to it.
The runner checks for them up front and says so if one is missing; recreate the
container with `docker compose down && docker compose up -d`.

To load every sample into one database for manual inspection instead of
checking them one at a time, use `make schema`.

## What the check does

The runner starts with `make clean-schema`, which drops every extension and
then every user schema, so neither a schema nor an extension left behind by an
earlier run can make a load fail on objects that already exist. The extensions
go first because a table an extension owns, PostGIS's `spatial_ref_sys` among
them, cannot be dropped while the extension is there. Then, for each sample,
it:

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
the SHA in `sample-db.mk`, and re-run `make test-samples`. omop is pinned to a
release tag rather than a branch tip, since the files at the tip do not load;
its loader says why.

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

## Coverage

Object counts of the loaded schemas.

### How the counts were taken

Counted 2026-08-08 on PostgreSQL 15.18, except:

- icingadb, rt, znuny, gitlab, hive, ranger, ambari, ovirt, and chado on 16.13,
  chado counted 2026-08-24.
- wso2is, nightingale, and danbooru 2026-08-29 on 15.17.
- openolat and inaturalist 2026-08-30 on 16.13.
- joomla and harbor 2026-09-01 on 16.13.
- bigbluebutton and listmonk 2026-09-11 on 16.13.
- dhis2 2026-09-15 on 15.18.
- coder, boundary, hatchet, thingsboard, glific, lago, calcom, and triggerdev
  2026-09-17 on 15.18.
- mattermost, lemmy, windmill, plausible, feedbin, and citizenlab 2026-09-17
  on 16.13, and dokploy, hyperswitch, documenso, langfuse, icinga_ido,
  openfire, bareos, opencms, and marquez 2026-09-18 on the same, and penpot
  2026-09-20 on 16.13 as well, and dcm4chee, kamailio, alfresco, roundcube,
  shenyu, and nacos the same day on the same.
- openreplay and logto 2026-09-20 on 16.13, and omero, concourse, affine, and
  teable 2026-09-21 on 15.18, and uyuni, lobehub, hexpm, omop, zed,
  gravitino, formbricks, hoppscotch, and streampark the same day on 16.13.
- The Sequences column on 15.18 throughout, and Triggers, added 2026-08-24, and
  Routines, added 2026-08-25, on 15.18 for every sample.

What each column holds:

- **Constraints** excludes foreign keys.
- **Types** counts enums and domains.
- **Sequences** counts standalone sequences only, since pistachio manages the
  sequence behind a serial or identity column as an attribute of that column
  rather than as an object of its own. Counting those too would add 2,292 more,
  886 of them gitlab's, 210 chado's, and 31 hexpm's, which declares no
  standalone sequence at all, as zed's 17 and gravitino's 1 do not either.
- **Triggers** excludes the internal triggers a foreign key installs and the
  clones PostgreSQL puts on each partition of a partitioned table's trigger, the
  same as what pistachio reads and dump writes.
- **Routines** counts what `--manage-routine` reads, so the aggregates, window
  functions, and `BEGIN ATOMIC` bodies pistachio leaves to `-- pista:execute`
  are out of it. lemmy is the sample that brings the last of those: 6 of its 80
  functions carry a SQL-standard body, so its column says 74.
- **Policies** are not a column. windmill declares 366 of them and logto 153,
  and no other sample turns row-level security on at all.

All counts are limited to the schemas the sample is checked with, and exclude
what an extension owns: the two views `pg_stat_statements` adds to sourcegraph's
schema are not sourcegraph's schema and pistachio does not read them either.

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
| lemmy | 58 | 573 | 290 | 113 | 101 | 0 | 16 | 1 | 66 | 74 |
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
| **Total** | **9,927** | **85,372** | **30,308** | **13,378** | **16,586** | **2,310** | **700** | **873** | **1,997** | **1,785** |

### Size

The 102 dumps come to about 285,000 lines of SQL. chado is 43,700 of them, the
longest dump of any sample, gitlab 34,700, and uyuni 19,700. gitlab is still
about a quarter of the constraints, a fifth of the indexes and the foreign
keys, a sixth of the columns, and a seventh of the tables; dhis2, uyuni,
openolat, musicbrainz, and discourse are the largest of what remains, and chado
is nearly all of the views.

gitlab is also why `clean-schema` drops tables a batch at a time rather than
cascading through `DROP SCHEMA`: a single statement takes locks on every object
it reaches, and gitlab's 1,422 tables and their indexes run the server out of
lock table space at the default `max_locks_per_transaction`. It is why
`reset-db` resets only `public` between samples, too.

### Shapes

Beyond size, the samples bring in shapes the hand-written fixtures do not
always reach.

- **Index methods and predicates**: partial and expression indexes and gin,
  gist, hash, and brin methods (musicbrainz, plus 12 partial and 2 gin indexes
  in synapse). citizenlab brings the first of the thirteen hnsw indexes the
  samples have between them, pgvector's, over its one `vector` column and
  naming `vector_cosine_ops` from the schema the extension sits in; affine
  brings two, one over each of its `vector(1024)` embedding columns, naming
  the same operator class unqualified; and lobehub brings the other ten, one
  over each of the `vector(1024)` columns it remembers a user in, naming it
  unqualified as well and leaving its eleventh such column, a document
  chunk's embedding, unindexed. langfuse brings four hash indexes, all over
  a `text` column, and two gin, one over a `text[]` column and one over
  `to_tsvector('english', content)`, which is the only expression index it has.
  logto brings one brin index over a timestamp, beside 179 btree, 11 of them
  partial and 13 over an expression, and a single gin. uyuni has one hash
  index, over the column holding a package capability's name, beside 931 btree,
  34 of them partial, and 3 gin, two naming `gin_trgm_ops` and the third over
  `to_tsvector`.
- **Index-heavy schemas**: danbooru's 456 indexes over 66 tables are seven to a
  table, denser than any other sample, 55 of them gin, 29 of those over an
  expression and 17 naming `gin_trgm_ops`, and 49 partial; mediawiki's 192 over
  64, only one of them partial and none over an expression; lago's 801 over 143,
  123 of them partial and 16 gin; feedbin's 161 over 44, every one of them
  btree, only 7 partial and 3 over an expression; opencms's 164 over 41 are
  four to a table like danbooru's but plainer: every one of them is btree, 120
  are non-unique, and not one is partial or over an expression. openreplay's
  268 over 62 are four to a table as well and lean the other way: 54 are gin
  and every one of those names `gin_trgm_ops`, which is what it searches a
  session's metadata with, and 55 are partial. dcm4chee's 290
  over 41 are seven to a table, as dense as danbooru's and plainer still: all
  btree, none partial, and the only three over an expression are `upper()` of
  a name or a description, which is how it searches case-insensitively.
  lobehub's 972 over 182 are five to a table, 958 of them btree with 72
  partial, 10 hnsw, and 4 gin, one of those over a key read out of a `jsonb`
  column with `jsonb_path_ops`. Two of its four other expression indexes cast
  a `jsonb` key to `numeric` and two wrap a nullable column in `COALESCE` to
  key on it.
- **Partial indexes**: 37 of lemmy's 290 and 88 of windmill's 392, which also
  has 31 gin indexes, and 50 of penpot's 169, where 35 of the predicates test a
  `deleted_at` or `archived_at` timestamp for NULL and two read a key out of a
  `jsonb` column. Every one of penpot's indexes is btree, and its 5 expression
  indexes are all over `jsonb` too, one of them over a `COALESCE` of two keys.
- **Unique indexes over an expression and a gin index over `to_tsvector`**: rt,
  plus mattermost's 11, six of them over the concatenation of two to five
  columns. hexpm's four gin indexes are three shapes at once: one names
  `gin_trgm_ops` over a plain column, two are over a `->` key of a `jsonb`
  column and one of those names `jsonb_path_ops`, and the fourth is over
  `to_tsvector('english', regexp_replace(...))` of such a key cast to text, a
  `to_tsvector` index with another function inside it.
- **gist indexes naming an operator class**: two that name `inet_ops` and one
  over four columns, which needs `btree_gist` (osm).
- **btree and gin indexes naming an operator class**: five of mattermost's nine
  btree indexes over `lower()` name `text_pattern_ops`, and its two gin indexes
  over a `->` expression name `jsonb_path_ops`. hoppscotch's four gin indexes
  split two ways: two name `gin_trgm_ops` over a title, resolving from the
  schema the sample installs `pg_trgm` into, and two are over a `jsonb`
  column at the default operator class, so the dump has to write one pair
  with the class and the other without.
- **gist indexes over a function the schema defines itself**: chado's three name
  `boxrange`, one of them partial and declared from another schema.
- **`NULLS NOT DISTINCT` and `INCLUDE`**: four unique indexes declared
  `NULLS NOT DISTINCT` and one index with an `INCLUDE` column (discourse), and
  one `NULLS NOT DISTINCT` index and eight with `INCLUDE` columns (lago), plus
  one more `INCLUDE` index in hexpm, over two columns and covering a third.
- **Storage parameters on an index**: concourse's two gin indexes, both over a
  `jsonb` column with `jsonb_path_ops` and both declared
  `WITH (FASTUPDATE = false)`, so its dump is where an index's storage
  parameter has to survive the round trip; hatchet's are on tables instead.
- **Exclusion constraints and unlogged tables**: demodb, which needs
  `btree_gist`, boundary, whose 19 exclusion constraints need it too, hatchet,
  whose 2 compare a range column with `&&`, lago, with one, and bigbluebutton,
  where every one of the 54 tables is unlogged. omero declares no exclusion
  constraint and leaves one table of its 161 unlogged, the one its triggers
  write the current administrative privileges into.
- **Stored generated columns**: bigbluebutton's 17, three of them over a
  function of its own that calls `unaccent`, and uyuni's one, which is over a
  field of the composite-typed column beside it.
- **Enums and domains**: dvdrental, pagila, employees, mediawiki, and icingadb,
  whose 13 types are 6 enums and 7 domains, each domain carrying a named CHECK,
  plus icinga_director, whose 20 enums come with one domain that carries two
  anonymous CHECKs, guacamole's 5 enums, listmonk's 14 over 16 tables, coder's
  61, which 73 columns are typed by, hatchet's 57, glific's 19, lago's 45, and
  calcom's 46 and triggerdev's 48, five columns between them typed as an array
  of one, mattermost's 7, which type 9 columns and one of which a partial
  index predicate casts to, lemmy's 16 over 24 columns, windmill's 33 over 57,
  plausible's 3, one of which is Oban's job state, and dokploy's 27, which 40
  of its columns are typed by and which are all quoted mixed-case names, as
  calcom's and triggerdev's are.
  hyperswitch is denser than any of them: 46 enums over 50 tables, typing 74
  columns and one array-of-enum column, and the 695 labels between them are
  lopsided too, since `CountryAlpha2` carries 249 of them and `Currency` 158.
  documenso's 30 over 51 tables are close behind, typing 34 columns, and
  langfuse's 35 over 74 tables type 45 of its columns with 116 labels between
  them.
  boundary declares 36 domains and no enum, 30 of the domains carry 39 CHECKs
  between them, and 1,039 of its 1,530 columns are typed by one.
  openreplay's 19 enums carry 374 labels between them and logto's 8 carry 32.
  omero's 12 types are 7 enums and 5 domains. The enums hold 157 labels between
  them, the SI-prefixed symbols of the units a measurement can carry, so many
  of the labels reach past ASCII, and each domain carries one anonymous CHECK
  bounding a number. concourse declares 5 enums, affine 11 with 40 labels, and
  teable 8. formbricks's 32 over 59 tables type 32 of its columns, one apiece,
  with 109 labels between them, and hoppscotch's 4 type 8 columns with 11.
  uyuni goes the other way for its size: 4 enums with 10 labels
  between them over 433 tables, since what it constrains it constrains with a
  CHECK instead.
- **Identity columns**: openreplay. Nineteen of its 62 tables draw their
  surrogate key from an `integer GENERATED BY DEFAULT AS IDENTITY` column
  rather than from a serial or a standalone sequence, so its dump is where the
  identity clause and the sequence behind it have to survive the round trip.
  uyuni has 21 of them, one per table, 18 on tables SUSE added and 3 on the
  RBAC tables in its `access` schema, while the tables around them take their
  key from one of its 207 standalone sequences instead.
- **Composite types**: ovirt declares 10 of them, more than any other sample,
  sourcegraph, chado, and coder 2 each, and marquez 1, which one of its views
  builds an array of with ROW(). uyuni has 1 as well, the four-field type its
  package versions are stored in: two columns are typed by it, two indexes read
  one of its fields, a generated column stores another, and 12 of its routines
  take or return it.
- **tsvector columns**: dvdrental, pagila.
- **A non-default collation**: musicbrainz.
- **Columns typed by a contrib extension**: sourcegraph, with 49 `citext`
  columns, and six extensions installed at once; lemmy, whose `comment.path` is
  an `ltree` and which installs `pg_trgm` and `pgcrypto` beside it; plausible,
  with 3 more `citext` columns; hexpm, with 1 more and five extensions
  installed for 36 tables, `citext`, `fuzzystrmatch`, `pg_trgm`, `pgcrypto`,
  and `uuid-ossp`, the last of which one column defaults through; and feedbin,
  with 2 `hstore` columns and
  `pg_stat_statements` installed beside them.
- **Columns typed by an extension that is not contrib**: discourse's three
  `halfvec` columns, which need pgvector, and the `geometry` columns that need
  PostGIS: osm's one `geometry(Polygon,4326)`, dhis2's one unmodified
  `geometry`, and inaturalist's 26, 8 of them carrying a modifier of their own
  and 8 gist indexes over them, and those modifiers are the only ones any sample
  reports in mixed case. citizenlab needs both, for one `vector` column and
  three `geography` ones, the only `geography` any sample declares. lobehub
  declares 11 `vector(1024)` columns, more than every other sample together,
  ten of them the embeddings of what it remembers about a user and one the
  embedding of a document chunk.
- **Foreign keys that all declare their referential actions**: all 171 of
  icinga_director's name both ON UPDATE and ON DELETE, in six combinations, and
  every one of calcom's 179 and triggerdev's 135 names ON UPDATE CASCADE and an
  ON DELETE action, CASCADE for most. 137 of glific's 142 name ON DELETE, 106 of
  them CASCADE, and 132 of dokploy's 133 name ON DELETE, 113 of them CASCADE
  and 19 SET NULL, while none of them names ON UPDATE at all. Every one of
  langfuse's 113 names ON UPDATE CASCADE as well, 90 of them with ON DELETE
  CASCADE and the other 23 with SET NULL. So does every one of logto's 152,
  149 of them with ON DELETE CASCADE, while all 84 of openreplay's name
  ON DELETE alone, 72 CASCADE and 12 SET NULL. lobehub is the largest of that
  shape: 541 of its 550 name ON DELETE, 417 CASCADE, 117 SET NULL and 7
  RESTRICT, and not one names ON UPDATE. uyuni mixes the two: 432 of its
  692 name ON DELETE, 371 CASCADE, 59 SET NULL and 2 RESTRICT, the other 260
  name nothing, and not one names ON UPDATE. formbricks names ON UPDATE
  CASCADE on all 91 of its and ON DELETE on 90, 77 CASCADE, 11 SET NULL and 2
  RESTRICT, and hoppscotch names ON UPDATE CASCADE on all 22 of its and
  ON DELETE CASCADE on 20.
- **Foreign keys over more than one column**: 3 of zed's 42, each naming two
  columns on both sides, so the dump has to write a pair of column lists back;
  one points a worktree's settings files at `worktrees(project_id, id)`, that
  table's composite primary key. formbricks has 9 of 91, more than any other
  sample, and every one of them pairs the id it references with the
  `"workspaceId"` beside it, so a row can only ever point at a row of its own
  tenant. One of the nine names the referencing columns in two spellings at
  once, `feedback_source_id` and `"workspaceId"`.
- **Column comments**: gravitino comments 183 of its 186 columns and every one
  of its 20 tables, the densest share of any sample; shenyu 360 of its 391
  columns and 6 of its 45 tables; nacos 102 of 175 and 10 of 16; glific 274 of
  its 590 columns; streampark 114 of its 290 and not one of its 26 tables.
- **Foreign keys that cross a schema boundary**: 20 of adventureworks' 90 span
  its five schemas, 12 of mimiciv's 51 point from `mimiciv_icu` into
  `mimiciv_hosp`, 4 of chado's 472 point from `frange` into `chado`, and every
  one of gitlab's partitions is attached across one.
- **Standalone sequences rather than serial columns**: ranger, whose 85 tables
  come with 84 of them, wso2apim, which mixes 104 of them in with serial
  columns, and wso2is, which declares 92 for its 172 tables and wires 87 of them
  into a column DEFAULT. dcm4chee declares 30 for its 41 tables and alfresco 38
  for its 45, and neither wires one into a DEFAULT at all: both are Java
  applications that ask for the next value themselves, so the sequence and the
  column it feeds are related only by name. roundcube goes the other way with
  the same syntax, declaring 8 and naming each one in the `nextval` DEFAULT of
  the column it belongs to, which is what `serial` would have written for it.
  omero declares 130 for its 161 tables, and is Java again: 129 are named
  `seq_` and the table they feed, only 2 are named in a DEFAULT, and the rest
  are read by the application alone. uyuni declares more than any other sample,
  207 for its 433 tables, and names exactly one of them in a DEFAULT: they are
  the Oracle schema's sequences, and Java asks each for the next value before
  it inserts. streampark declares 24 for its 26 tables and is Java too, but
  wires 23 of them into the DEFAULT of the `id` they feed and leaves the
  application to read only the twenty-fourth, `t_flink_app`'s. Every one of
  the 24 carries a non-default `START WITH 10000` and `MINVALUE 10000`, so
  the dump has to write both back.
- **Quoted mixed-case identifiers, so every name is case-sensitive**: hive's 84
  tables, where chinook has 11, hatchet's 72 of 133, calcom's 99 of 102 with 747
  of its 1,092 columns, triggerdev's 79 of 85 with 798 of 1,123, documenso,
  where all 51 tables and 314 of the 490 columns are, and bigbluebutton, where
  451 of 532 columns and half the tables and views are camelCase. langfuse is
  the Prisma schema that went the other way: only 2 of its 74 tables and 3 of
  its 757 columns are quoted and the rest are snake_case, though all 35 of its
  enum types are PascalCase. formbricks is back to the usual Prisma spelling,
  58 of its 59 tables and 258 of its 559 columns, and hoppscotch quotes every
  one of its 23 tables and 115 of its 171 columns.
- **Width without variety**: openolat, whose 382 tables are behind only gitlab
  and dhis2, has every one of its 1,239 indexes btree and every one of its 632
  foreign keys left at NO ACTION. omero is the same shape one size down: all
  811 of its indexes are btree, one of them partial and one over an expression,
  and all 696 of its foreign keys are at NO ACTION too. Those 696 over 161
  tables are more per table than any other sample, and 373 of its indexes are
  there for a key, behind 157 primary keys and 216 unique constraints.
- **A schema that is nearly all keys**: dhis2, where 461 primary keys and 464
  unique constraints back all but 30 of its 955 indexes, it declares no CHECK at
  all, and its 989 foreign keys are more than any sample but gitlab.
- **Foreign keys at the highest density**: omop's 39 tables carry 176 of them,
  4.5 to a table, ahead of omero's 4.3 over 161, and they nearly all point one
  way: every clinical event names the vocabulary entry that says what it was,
  so `concept` alone is referenced by 118 of the 176 and 3 are
  self-references. The rest of the schema is as lopsided. 28 primary keys over
  those 39 tables leave 11 unkeyed, and it declares no unique constraint and no
  CHECK at all, so its 28 constraints are 28 primary keys and nothing else. All
  98 of its indexes are btree, 28 backing a key and 70 not, and it has no view,
  sequence, routine, trigger, or type: its ids are integers an ETL supplies
  rather than serial columns, so nothing is counted behind them.
- **CHECK constraints written by hand**: affine declares 54 over its 72 tables,
  none of which Prisma wrote, since its schema language declares no CHECK at
  all and every one of them was added by a migration: most hold a text column
  to a fixed list of roles or states, and the rest pair two nullable columns so
  that either both are set or neither is. Its 16 triggers are hand-written for
  the same reason.
- **CHECK constraints written by the installer**: uyuni declares 109 by hand,
  nearly all of them holding a one-character column to `Y`/`N` or to a short
  list of codes, the way the Oracle schema it was ported from did. The other
  635 nobody wrote: the last statement of the install walks the catalog and
  adds a CHECK to every `varchar` column rejecting the empty string, which
  Oracle would have read as NULL. So 744 of the sample's 1,102 constraints are
  CHECKs, and the dump has to write back a schema whose constraint names were
  computed at install time.
- **A schema that barely keys at all**: mattermost backs its 86 tables with 85
  primary keys and 19 unique constraints, declares no CHECK, and leaves all but
  3 of the references between them to the application. mediawiki, temporal,
  imdb, dolphinscheduler, nightingale, joomla, hyperswitch, icinga_ido,
  bareos, opencms, kamailio, shenyu, nacos, gravitino, and streampark declare
  no foreign key at all, and openfire declares exactly one, over 35 tables.
  gravitino is the one of them that keys everything else: a primary key on
  each of its 20 tables and 19 unique constraints beside them back 39 of its
  52 indexes, so the references alone are what it leaves to the application.
  streampark keys as tightly for its size, a primary key on each of its 26
  tables and one unique constraint beside them. shenyu goes furthest:
  half its tables are unkeyed either way, 22 of 45 without a primary key, and
  its 45 tables carry 24 constraints between them, 23 primary keys and one
  CHECK. icinga_ido is the widest of them: 61
  tables and 791 columns, indexed 234 times and keyed by 61 primary keys and 33
  unique constraints, with every reference between them left to Icinga.
  kamailio has the most tables of any of them, 73, keyed by one primary key
  each and 36 unique constraints with no CHECK anywhere, though it does default
  380 of its 614 columns. Its schema is assembled one module at a time, so what
  ties the tables together lives in Kamailio's configuration rather than in the
  database. teable is the same shape from the other end of the stack: 62 tables
  keyed by 60 primary keys and no unique constraint at all, 2 CHECKs, and only
  23 foreign keys, with the rest of what ties its rows together left to the
  application. concourse leaves 15 of its 45 tables without a primary key.
- **Unique indexes standing in for unique constraints**: lobehub backs its 182
  tables with 182 primary keys and only 20 unique constraints, and writes the
  other 138 of its 340 unique indexes as a bare `CREATE UNIQUE INDEX`, which
  is what Drizzle emits. 54 of those carry a predicate and 2 are over an
  expression, which a constraint could not spell at all, so the dump has to
  write every one of them back as an index. The rest of the schema is tables
  and indexes and nothing else: no view, no enum, no domain, no composite
  type, no trigger, and no routine, one standalone sequence named in the
  DEFAULT of the one column it feeds, 35 CHECKs its migrations wrote by hand,
  and autovacuum storage parameters on the two tables that hold its
  embeddings. zed is the same shape one size down and stricter: 29 primary
  keys over 29 tables, no unique constraint and no CHECK at all, so all 29 of
  its constraints are primary keys, and 14 of its 43 unique indexes stand on
  their own. Of the 35 indexes that are not unique, 2 are gin over a name
  with `gin_trgm_ops`, the operator class resolving from the `public` the
  extension sits in as citizenlab's hnsw one does, and the other 33 are
  btree. formbricks is the shape again from Prisma rather than Drizzle: 58
  primary keys over 59 tables, no unique constraint and no CHECK at all, so
  its 58 constraints are primary keys and nothing else, and 51 of its 109
  unique indexes stand on their own. hoppscotch, Prisma as well, goes the
  other way for four of them: 21 primary keys and 4 unique constraints over
  23 tables leave 15 of its 40 unique indexes bare.
- **Materialized views**: adventureworks, pagila, listmonk, whose three views
  are all materialized, lago, mattermost, whose six are all materialized and
  one of which carries an index, and marquez, where one of the four is. hexpm
  has only two views and both are materialized, and both carry indexes, four
  between them: two are unique, which is what a concurrent refresh needs, and
  two order a column `DESC NULLS LAST`, one of those the second column of
  three.
- **Extensions in a schema of their own**: citizenlab puts all five of its in
  `shared_extensions` and qualifies every use with it, so 135 of its column
  defaults call `shared_extensions.gen_random_uuid()` and two of its indexes
  name an operator class from there. windmill does the same with `uuid-ossp`
  in an `extensions` schema, and lemmy installs its three into the schema it
  loads into, as hoppscotch does with `pg_trgm`. formbricks is the odd one:
  it installs pgvector into its own schema and uses nothing from it, since
  the three tables that carried its `vector(512)` columns were dropped by a
  later migration and the extension was left behind.
- **Views at scale**: chado's 1,864 are nearly ten times every other sample put
  together, and 1,832 of them are the Sequence Ontology views in its `so`
  schema, each selecting from tables in `chado`; bigbluebutton's 86 over 54
  tables are the most of any other sample, five of them selecting from another
  view. omero's 50 come next, one per link table, each counting the links a
  table holds and grouping them by owner.
- **Partitioned tables at scale**: gitlab declares 100 of them and attaches
  2,054 partitions, all of which live in schemas of their own. hatchet declares
  22, 20 by range and 2 by hash, and attaches none, since it creates its
  partitions at run time; 13 of its tables set autovacuum storage parameters.
  lago declares one and attaches five in its own schema, and triggerdev
  declares two by range and attaches none. thingsboard declares
  11, all by range, and attaches none for the same
  reason. windmill declares one by range and attaches four beside it. penpot
  declares one by hash and attaches all 16 of its partitions beside it, which
  is a quarter of the sample's 61 tables, and both of the parent's indexes are
  partitioned along with it.
- **Row-level security**: windmill turns it on for 38 of its 173 tables and
  backs them with 366 policies, 32 names reused across the tables. 98 are
  declared for ALL, 73 for SELECT, 71 for INSERT, 62 each for UPDATE and
  DELETE, and 77 carry a WITH CHECK as well as a USING. Every one of them names
  a role, `windmill_admin` or `windmill_user`, which the migrations create
  themselves; pistachio does not manage roles, but it has to write the names
  back. logto is the other sample that declares one, and it declares them the
  other way round: 77 of its 79 tables turn RLS on, and each one gets the same
  pair its seeder writes after every table, a RESTRICTIVE policy scoping the
  rows to the tenant and a permissive one over it. That is 153 policies, 76 of
  them restrictive, all of them FOR ALL, and none naming a role or carrying a
  WITH CHECK. windmill declares none that are restrictive, so logto's 76 are
  the only ones any sample has. No other sample declares a policy.
- **Table inheritance**: ledgersmb attaches 21 children with INHERITS, the only
  sample that does.
- **Triggers**: uyuni's 224 sit on 201 of its 433 tables, and 174 of them are
  the same trigger written 174 times: a row-level `BEFORE INSERT OR UPDATE`
  that stamps the row's `modified` column, each calling a function of its own
  rather than one shared between them, so 174 of its 412 routines exist to do
  nothing else. boundary's 741, more than any other sample, spread over 182
  of its 293 tables, 7 of them constraint triggers; gitlab's 388, one of them
  held in `ENABLE ALWAYS` state; logto's 85, 76 of which are the one its seeder
  puts on every table it creates; kea's 81 outnumber its 64 tables; coder's 30,
  lemmy's 66, bigbluebutton's 25, hatchet's 22, and ledgersmb's 11 come next.
  21 of hatchet's are statement-level triggers with transition tables, and 12
  sit on a partitioned table. One of marquez's 2 is an INSTEAD OF trigger on a
  view rather than a table, so its dump has to name the view. omero's 130 sit
  on 54 of its 161 tables, every one of them row-level, and 4 are deferrable
  constraint triggers that guard a privilege change. affine's 16 sit on 11 of
  its 72 tables and 4 of them fire `BEFORE UPDATE OF` a column list, one of
  four columns, so the dump has to write the list back. formbricks's 11 are
  that shape throughout: 10 of them fire `AFTER INSERT OR DELETE OR UPDATE OF`
  a column list, one of four columns, and all 11 call the same function with
  three arguments apiece, so the dump has to write the argument list back as
  well. streampark's 8 are the uyuni trigger done the cheap way: a row-level
  `BEFORE UPDATE` stamping a `modify_time`, but one function shared by all 8
  rather than one written per table. hoppscotch's 2 are
  `BEFORE INSERT OR UPDATE OF` a single column. concourse's 7 sit on 5
  tables: 2 call `pg_notify`, 4 create or drop a child table with INHERITS as a
  pipeline or a team comes and goes, so none of those children is in the dump,
  and 1 stands in for a foreign key.
- **Routines spread over a dozen schemas**: uyuni is checked with 13 schemas,
  more than any other sample. Its tables are in two of them, 426 in the one it
  loads into and 7 in `access`, and the other eleven hold routines alone: one
  per Oracle package it was ported from, `rhn_channel` down to `rhn_org`, plus
  `rpm` and `deb` for comparing a package version. 94 of its 412 routines sit
  in those eleven, so the dump has to carry the qualifier for the 318 in the
  main schema to call them.
- **Trigger functions in a schema of their own**: every one of lemmy's 66
  triggers sits on a table in `lemmy` and calls a function in `r`, the schema
  Lemmy's migration runner drops and rebuilds whenever those functions change,
  so the dump has to carry the qualifier for the plan to read it back.

### Routines

Routines are concentrated the same way. Fifty of the 102 samples declare
one at all, and uyuni's 412, gitlab's 337, boundary's 225, kea's and
musicbrainz's 130 each, and chado's 94 are 1,328 of the 1,785. Two in three of
them, 1,193, return `trigger`, though not every one of those has a trigger to
call it: musicbrainz's 89 do not, since its loader concatenates a file list
that leaves triggers out.

1,669 are written in plpgsql and 116 in sql. omero's 57, the next largest after
lemmy's 74, are 56 of the plpgsql and one of the sql, and 49 of them return
`trigger`; concourse and affine declare 7 each, 6 of concourse's and all of
affine's returning `trigger`, and teable 2. formbricks declares 2, a plpgsql
trigger function and an sql one taking two `jsonb` arguments, and hoppscotch
and streampark 1 each, both plpgsql and both returning `trigger`. uyuni's 412
are 407 plpgsql and 5 sql, and 224 of them return `trigger`. sourcegraph
declares one procedure,
thingsboard three, and lemmy and uyuni two each, the only procedures any sample
has, and inaturalist the only aggregate, which `--manage-routine` does not read
and so is in neither count; uyuni declares one as well, for the composite type
its loader creates, and it is out of the count for the same reason. bareos's
`decode_lstat` returns a 16-column `TABLE`, so the dump has to write the whole
column list back. Two of logto's ten are
declared `SET search_path`, so the dump has to carry the configuration along
with the body, and one of those two takes a VARIADIC argument. hexpm's two are
sql as well and both return `json`: one takes a VARIADIC `text[]` like logto's,
the other a polymorphic `anyelement`, so the dump has to write both argument
forms back. Only chado, kea,
boundary, and uyuni overload a name, 11 of them, 3, 1, and 1, though danbooru's
three, all sql, include a `lower(text[])` that shadows a built-in, and
documenso's `nanoid` gives all three of its arguments a default. Only sourcegraph,
ledgersmb, gitlab, coder, and boundary comment a routine, 137 between them, 124
of those boundary's.

## Load-time adjustments

Every loader runs its `psql` with `client_min_messages` raised to `warning`,
set once in `sample-db.mk` rather than sample by sample. Dumps drop what they
are about to create with `IF EXISTS`, declare an identifier past 63 characters
the server truncates, or hand an index to a constraint that renames it, and
each says so on a fresh database; none of it is about the schema under test,
and the runner passes a loader's stderr through. ranger raises the level
further, to `error`, from its `SAMPLES` record.

Some upstream dumps cannot be piped into `psql` as they are, either. The loader
targets strip only what is irrelevant to a schema round trip:

- **adventureworks**: `\copy` lines are dropped (the data lives in CSVs that are
  not fetched), along with the inline `Production.ProductReview` INSERT, whose
  foreign key targets would be missing.
- **affine**: the schema ships as a Prisma migration history like calcom's,
  123 directories replayed in name order into a schema of its own, but it needs
  two extensions in the search path as well, so it has a loader of its own
  rather than sharing `sample-db-prisma`. pgvector types its two `vector(1024)`
  embedding columns and the two hnsw indexes over them, and pgcrypto is
  installed and used to hash rows in the same migration file, so it has to
  resolve when that statement runs. Both are contrib or already required by
  another sample, and both are installed into `public` up front the way
  openreplay's are, with `public` second in the search path, since the
  `CREATE EXTENSION IF NOT EXISTS` the migrations write names no schema and
  gets nothing when another sample installed it somewhere else first.
- **bigbluebutton**: the file names no schema for almost everything it
  creates, so `search_path` places it, but three of its views are qualified
  with `public`. Those three would land outside the sample's schema and the
  views that select from them would then not resolve, so the qualifier is
  stripped. The file also installs `unaccent`, which a function behind three
  of its stored generated columns calls; it is contrib, so the official image
  already has it.
- **boundary**: the schema ships as migrations only, 290 files that Boundary
  replays in order: the two base files, then one directory per schema version
  in numeric order, with the files in each in name order. The repository
  tarball is fetched once and only the migrations directory is extracted,
  since fetching 290 files one at a time is slow. None of the files names a
  schema, so `boundary` is created up front and `search_path` places
  everything, the `citext`, `pgcrypto`, and `btree_gist` extensions included;
  all three are contrib. The migrations also assume the database holds
  Boundary alone: one runs a bare `analyze;` and another renames every unique
  constraint and foreign key in `pg_constraint`, whichever schema its table is
  in. Either stops the load when another sample's schema is already there, so
  boundary is the first sample in `SAMPLES`, loaded right after
  `clean-schema`.
- **calcom**, **triggerdev**: each schema ships as a Prisma migration history,
  596 and 828 directories each holding a `migration.sql`, replayed in name order
  into a schema of its own. The repository tarball is fetched once and only the
  migrations directory is extracted. A few files end without a semicolon or on a
  comment, so each is followed by a newline and one, and the `public` qualifier
  Prisma writes in some statements is stripped. Some migrations insert or update
  rows as well; they run, and the rows are not part of the check.
- **alfresco**: the schema ships as 11 create scripts, one per subsystem, and
  the order they run in is not their name order but the list in
  `db-schema-context.xml`, which the loader repeats: the repository tables
  first, since the rest key back into them, and the authorization tables last.
  Alfresco's runner rewrites each script before running it and the loader does
  the same two things. `${TRUE}` becomes `TRUE`, which is what `SchemaBootstrap`
  substitutes on a dialect with a boolean type and what 8 rows of bootstrap
  data here need. And a statement marked `--(optional)` is one its runner
  carries on past: there is exactly one, a `DROP TABLE` that means something
  only when an upgrade left the table behind, so on an empty database it can
  only fail and is dropped. The three marked `-- (optional)`, with a space,
  create a sequence, an index, and a table, and they stay. The scripts are
  CRLF, which psql reads as whitespace, so only the annotation match allows for
  the carriage return.
- **camunda**: the schema ships as one file per engine component and none of
  them create a schema, so `camunda` is created up front and the files are
  concatenated in dependency order (process engine, history, identity, then the
  case and decision engines with their history).
- **chado**: the file names no schema for the objects it creates, but four
  times partway through it sets `search_path` itself with `public` in it, which
  overrides anything `PGOPTIONS` passes in. The `public` in those four lines is
  rewritten to `chado`, the way hive's one line is. The lines that name
  `genetic_code`, `so`, and `frange` keep them: the file creates those three
  schemas itself, so the sample is checked with all four. Nine of its SQL
  functions are dropped, the six written against the `@` box operator that
  PostgreSQL 14 removed and the three that call one of those six; they error
  out on every version in the CI matrix.
  The 94 that load are part of the round trip like any other object. The
  `create_point` calls in the bodies of `boxrange` and `boxquery` are qualified
  with `chado.`, because PostgreSQL 17 runs `CREATE INDEX` with `search_path`
  set to `pg_catalog, pg_temp`: the three gist indexes over `boxrange` inline
  it, which re-resolves an unqualified `create_point` under that `search_path`
  and does not find it. With the calls qualified, every table, index,
  constraint, and view loads on all four versions of the CI matrix, the three
  gist indexes among them.
- **citizenlab**: the schema is Rails' `db/structure.sql` like discourse's, but
  multi-tenant: its five extensions live in a `shared_extensions` schema the
  file creates, and every use of them is qualified with it, so only the
  `public.` qualifier is stripped and `search_path` places the rest. It was
  dumped with `--clean`, so everything before the first `-- Name:` header is
  skipped, as it is for lago. Here that matters for more than tidiness: the
  preamble ends with `DROP SCHEMA IF EXISTS shared_extensions` and
  `DROP SCHEMA IF EXISTS public`, and the second would take every public sample
  with it in `make schema`. Skipping it also drops the `SET` lines `pg_dump`
  writes at the top, so `check_function_bodies` is passed in instead, and the
  `CREATE SCHEMA public` that opens the body is dropped, since `reset-db` has
  just created it.
- **clubdata**: the dump creates its own database and reconnects to it, which
  cannot be done mid-pipe. Those two lines are dropped; the rest creates the
  `cd` schema itself.
- **coder**: the dump is `pg_dump` output with the preamble stripped, and the
  preamble is where `pg_dump` turns `check_function_bodies` off. Left on, a
  plpgsql function that declares a variable of a table's row type stops the
  load, since the table comes later in the file, so the loader turns it off.
- **concourse**: the schema ships as golang-migrate migrations, 151 `.up.sql`
  files replayed in name order like mattermost's. Seven migrations beside them
  are Go rather than SQL and run in the same sequence; three only rewrite rows,
  and four change a table, dropping `teams.basic_auth`, renaming `teams.auth`
  to `legacy_auth` beside a new `auth`, and adding `resources.type` and
  `resource_pins.config`. Nothing in the SQL files depends on any of that, so
  the sample is checked without them, the way marquez is checked without its
  Java migrations' views, and its `teams` keeps the `basic_auth` column
  upstream drops. One migration installs pgcrypto and hashes rows with
  `digest()` in the same file, so pgcrypto is installed into `public` up front
  and `public` stays second in the search path.
- **dcm4chee**: the schema ships as plain DDL rather than migrations, in three
  files concatenated in dependency order: the tables and their 30 sequences,
  then the indexes over the foreign key columns, then the three
  case-insensitive ones, both of which need the tables.
- **demodb**: `btree_gist` is created first for the `bookings.routes` exclusion
  constraint, and the `\copy` lines are dropped.
- **dhis2**: the dump is the base schema Flyway starts from, a `pg_dump` that
  names no schema and no owner, so it loads into a schema of its own like the
  group below. It does not install PostGIS, which the one `geometry` column in
  `programstageinstance` needs, so the loader creates the extension first,
  `WITH SCHEMA public` since its own `search_path` names the sample's schema
  first, and leaves `public` in the search path for the type to resolve from.
- **discourse**, **osm**, **danbooru**, **inaturalist**, **feedbin**: all five
  ship their schema as Rails' `db/structure.sql`, which belongs in a schema of
  its own like the group below but is `pg_dump` output that empties
  `search_path` and qualifies every object with `public`, so neither
  `PGOPTIONS` nor hive's one-line rewrite reaches it. The line that empties
  `search_path` is dropped and the `public.` qualifier is stripped, which
  leaves every name unqualified for `search_path` to place. The
  `CREATE EXTENSION` lines say `WITH SCHEMA public` without a dot, so they are
  untouched and the types they own still resolve from `public`, which stays
  second in the search path. The tail of the file is Rails' own
  `SET search_path` followed by the migration versions it inserts into
  `schema_migrations`, which is data, so everything from that line on is
  dropped. danbooru installs five extensions of its own, `btree_gin`,
  `fuzzystrmatch`, `pg_trgm`, `pgcrypto`, and `pgstattuple`, but all five are
  contrib and the official image already has them, so it needs nothing installed
  the way discourse and osm do. inaturalist needs PostGIS, as osm does, and
  `uuid-ossp`, which is contrib and which 16 of its columns default through.
  feedbin installs three contrib extensions of its own, `hstore`,
  `pg_stat_statements`, and `uuid-ossp`.
- **documenso**: the schema ships as a Prisma migration history like calcom's
  and triggerdev's, 164 directories replayed through `sample-db-prisma`, so it
  needs no loader of its own. It installs `pg_trgm` and `pgcrypto`, both
  contrib, into the schema it loads into.
- **dokploy**: the schema ships as Drizzle migrations, the fifth migration tool
  in this list after Diesel, sqlx, golang-migrate, and Prisma. The repository
  tarball is fetched once and only the drizzle directory is extracted. Which
  files to replay comes from `meta/_journal.json` rather than from the
  directory listing, because the two do not agree: `0130_abandoned_dagger.sql`
  is on disk but not in the journal, so Drizzle never applies it, and replaying
  it adds a column that a later migration adds again, which stops the load. The
  journal lists its tags in the order Drizzle applies them, so they are read out
  of it and each file catted in turn, followed by a newline and a semicolon
  since a few end without one. None of the files names a schema, so `dokploy` is
  created up front and `search_path` places everything, but the foreign keys
  Drizzle writes qualify their target with `"public"`, which is stripped. Two
  of those foreign key names run past the 63 character identifier limit and the
  server truncates them, as it does wso2is's.
- **dvdrental**: the dump was taken by a `pg_dump` new enough to set
  `transaction_timeout` in its preamble, which 15 and 16 do not have, so that
  one line is dropped. It sets nothing the schema depends on.
- **omop**: the CDM's PostgreSQL DDL ships as four files under one directory,
  so `ddl`, `primary_keys`, `constraints`, and `indices` are fetched by name
  and concatenated in that order, the way mimiciv's three are. Every table
  name in all four carries an `@cdmDatabaseSchema` placeholder that OHDSI's R
  package fills in at install time, 529 of them, and the loader substitutes
  the sample's schema for it; nothing else in the files is a template. The pin
  is the v5.4.2 tag rather than the branch tip, the one sample where those
  differ for a reason other than where the schema lives: at the tip the
  primary key file leaves out `vocabulary`'s, which two foreign keys need, so
  the load stops on the first of them, and 5.5's files have the same gap.
- **formbricks**: the schema ships as a Prisma migration history like calcom's,
  175 directories holding a `migration.sql`, but `sample-db-prisma` cannot
  replay it. Two of its migrations ask the catalog whether an earlier rename
  has already happened and look in `public` to do it, and a later migration
  drops, unguarded, the index that rename produces: loaded anywhere else the
  guards answer no, the rename is skipped, and the drop fails on an index
  nobody renamed. `sample-db-formbricks` is `sample-db-prisma` with those two
  lookups pointed at `current_schema()`, and the unscoped `conname` lookup
  beside them narrowed to the sample's schema the way lemmy's is, so that a
  constraint another sample left behind cannot answer for this one. The
  schema also installs pgvector, which the official postgres image does not
  ship and `compose.yaml` adds for discourse and citizenlab already. Nothing
  the check reads uses it: the three tables that carried its `vector(512)`
  columns were dropped by a later migration and the extension was left
  behind.
- **glific**, **plausible**, **hexpm**: the schema is Ecto's `structure.sql`,
  the same `pg_dump` output as the group above, so it loads the same way. None
  has a `SET search_path` line before its migration versions, so those rows go
  into the sample's own `schema_migrations`. They are data, not schema.
  plausible installs `citext`, which is contrib and which three of its columns
  are typed by, and hexpm five, `citext`, `fuzzystrmatch`, `pg_trgm`,
  `pgcrypto`, and `uuid-ossp`, all contrib as well: one column is a `citext`,
  one gin index names the trgm operator class, and one column defaults through
  `uuid_generate_v4()` the way inaturalist's sixteen do, though hexpm's is
  nested inside a `json_build_object` cast to `jsonb`. `fuzzystrmatch` and
  `pgcrypto` the schema itself never names.
- **harbor**: the schema ships as one file per release, each a delta meant to
  be replayed by golang-migrate, which tracks what it has applied in a
  `schema_migrations` table of its own. One delta `ALTER TABLE`s that table
  directly, so a stand-in `schema_migrations` is created before the deltas run
  and dropped once they have; it is golang-migrate's bookkeeping, not part of
  Harbor's schema. A few deltas also omit their file's closing `;`, which
  merges the next file's opening statement into it once concatenated, so
  every file gets one appended regardless of whether it already ends in one.
- **hatchet**: the schema ships as three files, so `v0.sql`, `v1-core.sql`, and
  `v1-olap.sql` are concatenated in the order Hatchet's `sqlc.yaml` lists them.
  None of them names a schema, so `hatchet` is created up front and
  `search_path` places everything.
- **hive**: the dump belongs in a schema of its own like the group below, but
  it is `pg_dump` output that sets `search_path` to `public` itself, which
  overrides anything `PGOPTIONS` passes in. That one line is rewritten to name
  the `hive` schema.
- **hoppscotch**: the schema ships as a Prisma migration history like calcom's,
  22 directories replayed through `sample-db-prisma`, so it needs no loader of
  its own. It qualifies nothing with `public`, so the sed that strips the
  qualifier has nothing to strip, and its `CREATE EXTENSION IF NOT EXISTS
  pg_trgm` lands in the sample's own schema, which is where the two gin
  indexes over a title then resolve `gin_trgm_ops` from.
- **hyperswitch**: the schema ships as Diesel migrations, 530 directories each
  holding an `up.sql`, replayed in name order. The repository tarball is fetched
  once and only the migrations directory is extracted, the way lemmy's is. Every
  directory but Diesel's own `00000000000000_diesel_initial_setup` is named for
  a date, so plain name order is the order Diesel applies them in. Four of the
  files end without a semicolon, so each is followed by a newline and one.
  Nothing in them names a schema, qualifies anything with `public`, or installs
  an extension, so `search_path` places the lot.
- **imdb**: the schema and its foreign key indexes ship as two files, so
  `schema.sql` and `fkindexes.sql` are concatenated.
- **joomla**: the schema ships as three files that must load in order --
  `base.sql`, `extensions.sql`, and `supports.sql`, the last of which
  references content types `extensions.sql` creates -- so they are
  concatenated. None of the three create a schema or set `search_path`
  themselves, and every table name carries the literal `#__` prefix Joomla
  substitutes at install time; quoted, it is just an ordinary identifier and
  needs no rewriting.
- **kamailio**: the schema is one file per module rather than one per release,
  and which modules a database gets is the installer's choice: `kamdbctl`
  creates the standard set always and asks about the presence, extra, and uid
  sets. The loader concatenates all four, in the order `kamdbctl.base` lists
  them, which puts `standard` first because every file writes a row into the
  `version` table it creates. The five files left over are four IMS ones and
  `matrix`, which `kamdbctl` does not offer. A schema of its own matters more
  here than usual: this is where the generic names live, `domain`, `group`,
  `location`, `subscriber`, `uri`, `address`, `version`.
- **lago**: the schema is Rails' `db/structure.sql` like discourse's, and loads
  the same way with two things taken out first. It was dumped with `--clean`,
  so everything before the first `-- Name:` header, about 1,400 lines of
  `DROP ... IF EXISTS` and placeholder views, is skipped; with `public` second
  in `search_path` those could reach another sample's objects in `make schema`.
  It also installs `pg_partman`, which is not contrib, into a schema of its own
  with one template table, so every statement that names partman is dropped.
- **langfuse**: the schema ships as a Prisma migration history like calcom's,
  triggerdev's, and documenso's, 438 directories replayed through
  `sample-db-prisma`, so it needs no loader of its own. It installs no
  extension and qualifies nothing with `public`, so the sed that strips the
  qualifier has nothing to strip. One of its index names runs past 63
  characters, which the server truncates, and one migration turns a unique
  index into a primary key with `ADD CONSTRAINT ... USING INDEX`, which renames
  the index; neither object survives into the schema the check reads, since a
  later migration drops the table behind them.
- **lemmy**: the schema ships as Diesel migrations, 342 directories each holding
  an `up.sql`, and that is only half of it: every trigger function lives in a
  schema named `r` that Lemmy's own runner builds afterwards out of two files.
  So the migrations are followed by `CREATE SCHEMA r` and those two files, in
  the order `schema_setup/mod.rs` lists them. The repository tarball is fetched
  once and both paths are extracted from it. None of the files names a schema,
  so `lemmy` is created up front and `search_path` places everything, the
  contrib extensions `ltree`, `pg_trgm`, and `pgcrypto` included; the migrations
  create a `utils` schema themselves, so the sample is checked with all three.
  Some migrations qualify a table or a function with `public`, which is
  stripped. One of them puts a trigger on `__diesel_schema_migrations`, Diesel's
  bookkeeping table, which the CLI creates rather than a migration, so a
  stand-in is created before the migrations run and dropped once they have, the
  way harbor's is. Twenty-two turn a table's indexes off around a bulk update
  and find the table with `SELECT oid FROM pg_class WHERE relname = '<table>'`,
  naming no schema: upstream Lemmy owns its database, but here the samples
  before it are still there and `comment` alone matches several, so those
  lookups are scoped to the `lemmy` schema. The one `relname LIKE` inside a
  function body is left alone.
- **lobehub**: the schema ships as Drizzle migrations like dokploy's and is
  read the same way, from `meta/_journal.json` rather than from the directory,
  because the two do not agree here either:
  `0065_add_document_fields.sql` is on disk but not in the journal, left
  behind by the rename that made it `0066_add_document_fields.sql`, and the
  two differ in the ON DELETE action of the foreign key they add, so replaying
  the directory would take the older one. Each file is followed by a newline
  and a semicolon since a few end without one, and the `public` qualifier
  Drizzle writes into a foreign key's target is stripped, along with the one
  the `to_regclass` and `::regclass` guards in a handful of hand-written
  migrations carry, which leaves those guards reading through `search_path`.
  Twelve other guards look a constraint up by name alone,
  `SELECT 1 FROM pg_constraint WHERE conname = '<name>'`, and skip the
  `ALTER TABLE` after them when they find one: upstream LobeHub owns its
  database, but here the samples before it are still there, and a name one of
  them already uses -- `users_email_unique` is one -- would cost this sample
  a constraint without saying so, the way lemmy's unscoped `pg_class` lookups
  would, so they are scoped to the sample's schema. The project was LobeChat
  before it was renamed, and codeload names the archive's top directory after
  the repository as it is now, so the URL and the prefix both say `lobehub`.
  It has a loader of its own rather than sharing dokploy's
  for two reasons. pgvector types its 11 `vector(1024)` columns and the 10
  hnsw indexes over them, so it is installed into `public` up front the way
  affine's is, with `public` second in the search path, since the
  `CREATE EXTENSION IF NOT EXISTS vector` a migration writes names no schema.
  And two migrations need ParadeDB's `pg_search`, which is not contrib and
  which the official image does not ship, so they are skipped: one installs
  the extension and the other writes 14 bm25 indexes with it, and nothing
  else in the history reads them.
- **logto**: the schema is one file per table under `packages/schemas/tables`,
  which Logto's CLI seeds rather than replaying migrations, so the repository
  tarball is fetched once and that directory is extracted, along with the model
  file beside it that holds the one table the directory does not. The order the
  files load in is the one `compareQuery` sorts them into, the ones carrying an
  `init_order` comment first by that number and the rest by name, so the number
  is cut out of each file and sorted on its own and a file without one is given
  a number past every real order. `tenants` comes first at order 0, and it is
  the table Logto keeps as a template literal in a TypeScript model rather than
  in `tables/`, so the SQL is cut out of the literal and the one placeholder in
  it substituted with the tag Logto's own enum gives it; every other table keys
  back into it. A few files end without a semicolon, so each is followed by a
  newline and one. `_after_each.sql` is emitted after every file that does not
  say `/* no_after_each */`, with `${name}` substituted, which is what the CLI
  does with it and where the 153 policies come from. `_before_all.sql` is not
  run: all it does is create the role the grants in `_after_all.sql` name, and
  roles and grants are out of pistachio's scope, so those grant and revoke
  statements are dropped too and only the two around them are kept, the ones
  that turn RLS on for `tenants` and write its policy. None of the files
  creates a schema, so `logto` is created up front and `search_path` places
  everything; the handful that qualify a table or a function with `public` have
  the qualifier stripped, and the two functions declared
  `set search_path = public` are pointed at the sample's schema, which is the
  schema Logto means by it.
- **marquez**: the schema ships as Flyway migrations, 81 versioned files and 3
  repeatable ones in one directory, which the repository tarball is fetched
  once for. Flyway applies the versioned files in version order rather than
  name order, since V10 comes after V9 and V17.1 sits between V17 and V18, so
  the version is cut out of each name and sorted on its own; the repeatable
  files, the ones named `R__`, follow in name order, where Flyway runs them.
  A few end without a semicolon, so each is followed by a newline and one.
  Marquez also ships seven Java migrations that Flyway runs in the same
  sequence: six backfill rows and the seventh creates facet views, so the
  sample is checked without those views, the way musicbrainz is checked without
  the triggers its file list leaves out.
- **mattermost**: the schema ships as golang-migrate migrations, 227 `.up.sql`
  files replayed in name order. The repository tarball is fetched once and only
  the migrations directory is extracted, since fetching 227 files one at a time
  is slow. None of the files names a schema, and the guards they write against
  `information_schema` all say `current_schema()`, so `mattermost` is created up
  front and `search_path` places everything. Eight of the files end without a
  semicolon, so each is followed by a newline and one.
- **mediawiki**, **synapse**, **temporal**, **icingadb**, **icinga_ido**,
  **rt**, **znuny**, **ranger**, **ambari**, **ovirt**, **gitlab**,
  **ledgersmb**, **koji**, **kea**, **dolphinscheduler**, **wso2apim**,
  **icinga_director**, **openfire**, **bareos**, **opencms**, **roundcube**,
  **flowable**, **ejabberd**, **guacamole**, **dotcms**, **wso2is**,
  **nightingale**, **openolat**, **listmonk**, **dhis2**, **coder**, **nacos**,
  **gravitino**: these
  dumps name no schema at all, so whichever schema comes first in `search_path`
  gets them.
  Each is loaded into a schema of its own instead of `public`, so that
  `make schema`, which puts every sample in one database, does not stack them on
  top of the other public samples (mediawiki and pagila both define `actor` and
  `category`). gitlab creates
  `gitlab_partitions_static` and `gitlab_partitions_dynamic` itself and never
  qualifies anything with `public`, so its 1,083 top-level tables follow
  `search_path` into `gitlab` while its partitions stay in the two schemas it
  named.
- **mimiciv**: the schema ships as three files, so `create.sql` (tables),
  `constraint.sql` (primary and foreign keys), and `index.sql` are concatenated
  in that order.
- **musicbrainz**: the schema ships as one file per object kind and none of them
  create the schema, so `musicbrainz` is created up front and the files are
  concatenated in dependency order (extensions and collation, search
  configuration, types, tables, functions, then keys, indexes, constraints, and
  views).
- **omero**: a fresh database is the four files `omero db script`
  concatenates, in that order: `psql-header.sql`, which opens the transaction
  and declares the domains and the unit enums, `schema.sql`, the tables
  Hibernate generates, `views.sql`, and `psql-footer.sql`, which adds the
  indexes, the functions, and the triggers and commits. `omero db script`
  renders the header and the footer through Python's `%` formatting, so every
  literal percent sign in them is written twice and the loader undoubles them.
  That is the whole substitution: the header's `%(TIME)s` and the rest of its
  placeholders sit in comments, and the footer's one `@ROOTPASS@` goes into a
  row of the `password` table rather than into the schema, so it loads as the
  literal string. Nothing in the files names a schema or an extension, so
  `omero` is created up front and `search_path` places everything.
- **openreplay**: the schema is one file that qualifies almost everything it
  creates with `public`, so the qualifier is stripped the way it is for the
  group above and `search_path` places the rest; the four schemas it names
  itself it also creates, so the sample is checked with all five. It opens by
  asking whether `public.tenants` is already there and quitting with `\q` when
  it is, which is how OpenReplay refuses to re-run over an installed database.
  That lookup is scoped to the sample's schema, the way windmill's and lemmy's
  are: left naming `public` it asks about a schema this sample never writes to,
  and in `make schema` it could match another sample's table and quietly load
  nothing. The `SET client_min_messages TO NOTICE` on its second line is
  dropped, for the same reason every loader raises the level to `warning`. It
  installs `pg_trgm`, which its 54 gin indexes all name `gin_trgm_ops` from,
  and `pgcrypto`. Both are contrib, so the official image has them, but the
  file says `IF NOT EXISTS` and names no schema, so both are installed into
  `public` up front the way penpot's `uuid-ossp` is, and `public` stays second
  in the search path for the operator class to resolve from.
- **penpot**: the schema ships as 165 SQL migration files in one directory,
  which the repository tarball is fetched once for, along with the
  `migrations.clj` beside it. That file, not the directory listing, says which
  files to replay and in which order, and the two do not agree: three files on
  disk are not in the list, and replaying `XXXX-drop-obsolete-tables.sql`
  alone would drop a table and three columns the sample is meant to carry.
  Nor is the list in name order, since six files share their number with
  another and are listed the other way round. It names each file as a resource
  path under `app/`, which the extracted directory is the tail of, so that
  prefix is cut. Two files end without a trailing newline, so each is followed
  by a newline and a semicolon. Penpot's two Clojure migrations run in the same
  sequence and both rewrite rows rather than schema, so the sample is checked
  without them, the way marquez is checked without Flyway's Java migrations.
  The first migration installs `uuid-ossp`, which is contrib, but it says
  `IF NOT EXISTS` and names no schema, so it is installed into `public` up
  front and `public` stays second in the search path for the one column default
  that still calls `uuid_generate_v4` to resolve from.
- **ranger**: the dump drops every object it is about to create with
  `IF EXISTS` and commits outside a transaction, which adds a warning per
  statement, so `client_min_messages` is raised from `warning` to `error` for
  the load, the one sample that moves it at all.
- **shenyu**: the schema ships as one file that loads like the group above but
  qualifies every name in it with `public`, the sequences and the `DEFAULT
  nextval` that reads them included, so the qualifier is stripped the way
  sample-db-prisma strips it. That is not only about where the objects land:
  the file opens each table with `DROP TABLE IF EXISTS "public"."<name>"`, and
  in `make schema`, where every sample shares one database, several of those
  names belong to another sample.
- **streampark**: the schema ships as one file that qualifies every name in it
  with `"public"` exactly as shenyu's does, the sequences and the `DEFAULT
  nextval` that reads them included, so it gets a loader of shenyu's shape and
  the quoted qualifier is stripped. It opens with a `DROP TABLE IF EXISTS`
  per table and a `DROP SEQUENCE IF EXISTS` per sequence, but only 23 of the
  24 sequences it goes on to create, so a second load into a schema it
  already owns stops on `streampark_t_resource_id_seq`. The check loads each
  sample once into a schema of its own, so it never reaches that.
- **teable**: the schema ships as a Prisma migration history like calcom's,
  115 directories replayed in name order into a schema of its own.
- **thingsboard**: the schema ships as one file per part, loaded in the order
  ThingsBoard's installer runs them, with the views before the functions that
  declare variables of their row types. `schema-ts-latest-psql.sql` is left
  out, since only the migration from Cassandra reads it and
  `schema-entities.sql` already creates its table.
- **uyuni**: the schema is not a file but a source tree.
  `schema/spacewalk/common` holds a file per table, view, and reference data
  load shared with the Oracle port it came from, `schema/spacewalk/postgres` the
  PostgreSQL side -- the enums, the functions, the triggers, and one schema per
  Oracle package it rewrote -- and beside them a `.deps` file per directory
  saying what has to come first. `blend`, the Python tool in the tree, reads
  those and writes one `main.sql`. The loader runs that build rather than
  reimplementing the order, so this is the sample that needs GNU `make` and
  `python3`; the repository tarball is fetched once and only the schema tree and
  one file from the container image come out of it. One file in the tree is a
  template rather than SQL, `rhnVersionInfo.pre`, which `Makefile.schema` fills
  in with the schema name, version, and release before the build runs and which
  blend stops without, so the loader substitutes it the same way; the values
  only reach a row. `evr_t`, the composite type `rhnPackageEVR.evr` is declared
  with, is not in the tree at all -- the server container's entrypoint creates
  it, with the functions, operators, and operator class that compare two of them
  -- so the SQL in that script runs first, taken out of the heredoc it is
  wrapped in with the shell's escaping undone. Two of those functions call
  `rpm.vercmp` and `deb.debvercmp`, which SUSE ships as C extensions; both
  bodies are plpgsql, so nothing resolves them when the function is created and
  nothing the check runs calls them. The tree names no schema, so `uyuni` is
  created up front and `search_path` places everything; it creates twelve more
  itself, so the sample is checked with all thirteen. Two REFERENCES qualify
  `public`, which is where upstream installs, so the qualifier is stripped.
  `pg_trgm`, which the two indexes that name `gin_trgm_ops` need -- the third
  gin index is over `to_tsvector` and takes the built-in `tsvector_ops` --
  is installed into `public` up front the way concourse's `pgcrypto` is, and the
  tree's own `CREATE EXTENSION pg_trgm` is dropped, since it names no schema and
  no IF NOT EXISTS and would stop the load in `make schema` once another sample
  has installed it. The last thing the build appends walks the catalog and puts
  a CHECK constraint on every `varchar` column, rejecting the empty string
  Oracle would have read as NULL; it takes the tables `current_user` owns that
  `search_path` can see, which upstream is Uyuni's alone and here would reach
  every sample in `public`, so that lookup is scoped to the sample's schema, the
  way lemmy's are. The 635 constraints it writes are the same either way, since
  the only other schema in the search path is the empty `public` the runner has
  just recreated. Every `commit` in the reference data loads warns that there is
  no transaction in progress, so `client_min_messages` is raised to `error` as
  it is for ranger.
- **windmill**: the schema ships as sqlx migrations, 661 `.up.sql` files
  replayed in name order. The repository tarball is fetched once and only the
  migrations directory is extracted. It names no schema, so `windmill` is
  created up front and `search_path` places everything; the five files that
  qualify something with `public` have the qualifier stripped. The migrations
  create an `extensions` schema of their own and install `uuid-ossp` there,
  which is contrib, so `extensions` stays second in the search path for the
  column defaults that call it and is not part of the check. They also create
  the `windmill_user` and `windmill_admin` roles, each in a `DO` block that
  swallows the error when the role is already there; pistachio does not manage
  roles, but 366 of Windmill's policies name one, so they have to exist for the
  policies to load. One migration reads `_sqlx_migrations`, sqlx's own
  bookkeeping table, so a stand-in is created and dropped around the run the way
  harbor's is, and several functions are declared before the tables they read,
  so `check_function_bodies` is turned off as it is for coder. Four catalog
  lookups assume Windmill owns the database and are scoped to the sample's
  schema: two read `information_schema.columns` with no schema filter, one of
  them to build `queue_view` out of whichever columns it finds, which picks up
  another sample's `queue`; two name `schemaname = 'public'` against
  `pg_policies`, which finds nothing here, and one of those is the migration
  that rewrites every policy reading a session GUC, so without the rewrite the
  sample would carry its 366 policies with the wrong expressions in them.
- **wso2is**: five of its index names run past the 63 character identifier
  limit, and the server truncates them.
- **zed**: the schema is a dump of Zed's collaboration server, which its own
  `cargo xtask db dump-schema` writes, so it is `pg_dump` output qualified
  with `public` and loads the way the Rails group above does, minus the two
  things they need: it has no line emptying `search_path` and no migration
  versions at the tail, so only the qualifier is stripped. It installs
  `pg_trgm`, which is contrib, for the two gin indexes it declares over a name
  with `public.gin_trgm_ops`; the qualifier comes off that too and the
  operator class resolves from `public`, which stays second in the search
  path. Its other 76 indexes are btree.
- **znuny**: the schema ships as two files, so `schema.postgresql.sql` (tables
  and indexes) and `schema-post.postgresql.sql` (foreign keys, which need every
  table to exist) are concatenated in that order.

None of these touch table, column, index, constraint, view, type, or routine
definitions, chado's nine unloadable functions, the `search_path` two of
logto's functions are declared with, and lobehub's 14 bm25 indexes aside, so
the round trip still covers the full schema. The bm25 indexes are the one case
where a sample is checked against less than its upstream schema, and the
reason is the server rather than pistachio: the extension that defines the
access method is not one the official image can install.

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
5. Leave `CHANGELOG.md` alone. A sample is test-only, and nothing about it
   reaches someone using pista.

A `DRIFT` result is the interesting outcome: it means pistachio reads or writes
that schema incorrectly. Fix the catalog reader, the parser, or the diff before
adding the sample, rather than trimming the schema to make it pass.
