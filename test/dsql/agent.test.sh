#!/usr/bin/env bash
# DSQL scenario: the grid investigation schema of the aws-samples agent
# sample. The steps build it, add a table with a foreign key and a check,
# change tables of the sample, drop some of them, and drop everything.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
source "$SCRIPT_DIR/helper.sh"

BASE="$DSQL_SAMPLES/agent.sql"

reset_db

# --- Step 0: build the sample from nothing ---
run_step "00 build the sample" "$(cat <<'EOS'
CREATE TABLE public.grid_incidents (
CREATE TABLE public.maintenance_log (
CREATE INDEX ASYNC idx_feeder_events_feeder_time ON public.feeder_events (feeder_id, recorded_at);
CREATE INDEX ASYNC idx_maintenance_feeder ON public.maintenance_log (feeder_id, scheduled_at);
EOS
)" "$BASE" || true

assert_dump_round_trip "01 dump plans clean" || true

# --- Step 2: a new table with a foreign key, a check and an index ---
cat > "$WORK/notes.sql" <<'EOS'
CREATE TABLE incident_notes (
    note_id     TEXT PRIMARY KEY,
    incident_id TEXT NOT NULL,
    body        TEXT,
    CONSTRAINT incident_notes_body_check CHECK (body <> '')
);
ALTER TABLE incident_notes ADD CONSTRAINT incident_notes_incident_fk
    FOREIGN KEY (incident_id) REFERENCES grid_incidents (incident_id);
CREATE INDEX incident_notes_incident_idx ON incident_notes (incident_id);
EOS
run_step "02 create a table with a foreign key" "$(cat <<'EOS'
CREATE TABLE public.incident_notes (
ALTER TABLE ONLY public.incident_notes ADD CONSTRAINT incident_notes_incident_fk FOREIGN KEY (incident_id) REFERENCES grid_incidents (incident_id) NOT VALID;
ALTER TABLE ASYNC public.incident_notes VALIDATE CONSTRAINT incident_notes_incident_fk;
CREATE INDEX ASYNC incident_notes_incident_idx ON public.incident_notes (incident_id);
EOS
)" "$BASE" "$WORK/notes.sql" || true

# --- Step 3: change tables of the sample: a column with a default, a check
#     and a unique constraint on an existing table, a changed index, a
#     changed foreign key, and a comment ---
derive "$WORK/edit.sql" "$BASE" '
  s/(    created_at      TIMESTAMPTZ DEFAULT NOW\(\))/$1,\n    priority        INTEGER DEFAULT 0,\n    CONSTRAINT grid_incidents_severity_check CHECK (severity IN (\x27low\x27, \x27medium\x27, \x27high\x27, \x27critical\x27)),\n    CONSTRAINT grid_incidents_feeder_started_key UNIQUE (feeder_id, started_at)/;
  s/ON grid_incidents \(feeder_id, started_at\)/ON grid_incidents (feeder_id, severity)/;
'
derive "$WORK/notes_edit.sql" "$WORK/notes.sql" '
  s/\(incident_id\);\nCREATE INDEX/(incident_id) ON DELETE CASCADE;\nCREATE INDEX/;
  $_ .= "COMMENT ON TABLE incident_notes IS \x27notes on an incident\x27;\n";
'
run_step "03 edit tables of the sample" "$(cat <<'EOS'
ALTER TABLE public.grid_incidents ADD COLUMN priority integer;
ALTER TABLE public.grid_incidents ALTER COLUMN priority SET DEFAULT 0;
ALTER TABLE public.grid_incidents ADD CONSTRAINT grid_incidents_severity_check CHECK (severity IN ('low', 'medium', 'high', 'critical')) NOT VALID;
ALTER TABLE ASYNC public.grid_incidents VALIDATE CONSTRAINT grid_incidents_severity_check;
CREATE UNIQUE INDEX ASYNC grid_incidents_feeder_started_key ON public.grid_incidents (feeder_id, started_at);
ALTER TABLE public.grid_incidents ADD CONSTRAINT grid_incidents_feeder_started_key UNIQUE USING INDEX grid_incidents_feeder_started_key;
DROP INDEX public.idx_incidents_feeder;
CREATE INDEX ASYNC idx_incidents_feeder ON public.grid_incidents (feeder_id, severity);
ALTER TABLE public.incident_notes DROP CONSTRAINT incident_notes_incident_fk;
ALTER TABLE ONLY public.incident_notes ADD CONSTRAINT incident_notes_incident_fk FOREIGN KEY (incident_id) REFERENCES grid_incidents (incident_id) ON DELETE CASCADE NOT VALID;
ALTER TABLE ASYNC public.incident_notes VALIDATE CONSTRAINT incident_notes_incident_fk;
COMMENT ON TABLE public.incident_notes IS 'notes on an incident';
EOS
)" "$WORK/edit.sql" "$WORK/notes_edit.sql" || true

assert_dump_round_trip "04 dump plans clean after the edits" || true

# --- Step 5: drop a column, an index, a check, a table, and the table that
#     holds the foreign key ---
derive "$WORK/drop.sql" "$WORK/edit.sql" '
  s/    description     TEXT,\n//;
  s/,\n    CONSTRAINT grid_incidents_severity_check CHECK \([^\n]*\)\)//;
  s/CREATE INDEX ASYNC IF NOT EXISTS idx_maintenance_feeder[^\n]*\n//;
  s/CREATE TABLE IF NOT EXISTS incident_weather \(.*?\n\);\n//s;
  s/CREATE INDEX ASYNC IF NOT EXISTS idx_weather_feeder_time[^\n]*\n//;
  s/GRANT SELECT ON incident_weather TO grid_reader;\n//;
'
run_step "05 drop objects" "$(cat <<'EOS'
ALTER TABLE public.grid_incidents DROP COLUMN description;
ALTER TABLE public.grid_incidents DROP CONSTRAINT grid_incidents_severity_check;
DROP INDEX public.idx_maintenance_feeder;
DROP TABLE public.incident_weather;
DROP TABLE public.incident_notes;
EOS
)" "$WORK/drop.sql" || true

# --- Step 6: drop everything ---
run_step "06 drop everything" "$(cat <<'EOS'
DROP TABLE public.grid_incidents;
DROP TABLE public.maintenance_log;
EOS
)" "$WORK/empty.sql" || true

summary
