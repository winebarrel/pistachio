package pistachio

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/pistachio/parser"
)

// planFileVersion is the format of the file plan --out writes and apply-from
// reads. A file of another version is refused rather than read as this one.
//
// Raise it whenever what a plan file holds or how it is read changes, the
// state hash included. The hash is taken over the model as JSON, so adding a
// field to a model struct changes it for an unchanged database: a file
// written before that change would otherwise be reported as drift, and the
// operator would go looking at the database for it.
const planFileVersion = 1

// planFile is what plan --out writes: the statements apply-from is to run, the
// scope they were computed under, and the fingerprint of the schema they were
// computed against.
//
// The statements are held apart the way apply runs them rather than as one
// script. Pre-SQL runs before the search_path is set and outside the managed
// DDL, the CONCURRENTLY pre-SQL only when there is CONCURRENTLY index DDL, and
// an execute statement carries its directive into the output.
//
// pista writes this file and pista reads it. There is no schema published for
// it, and no promise beyond the version above.
type planFile struct {
	Version int `json:"version"`
	// ServerVersion is the major version of the server the plan was computed
	// against. It decides what the catalog reads and what DDL is valid, so a
	// plan file is not carried to a server of another major version.
	ServerVersion int `json:"server_version"`
	// Scope is what apply-from reads the database with. Taking it from the
	// file rather than the command line is what makes the state hash mean
	// anything: a read under other filters is a different read.
	Scope ScopeOptions `json:"scope"`
	// StateHash fingerprints the current-side schema the statements were
	// computed against.
	StateHash            string                `json:"state_hash"`
	PreSQL               string                `json:"pre_sql"`
	ConcurrentlyPreSQL   string                `json:"concurrently_pre_sql"`
	HasConcurrentlyIndex bool                  `json:"has_concurrently_index"`
	Stmts                []string              `json:"statements"`
	ExecuteStmts         []*parser.ExecuteStmt `json:"execute_statements"`
	DisallowedDrops      []string              `json:"disallowed_drops"`
	Ignored              []string              `json:"ignored"`
}

// writePlanFile writes the plan file as indented JSON, with the newline a text
// file ends on.
func writePlanFile(path string, plan *planFile) error {
	encoded, err := json.Marshal(plan, jsontext.WithIndent("  "), json.Deterministic(true))
	if err != nil {
		return fmt.Errorf("failed to encode the plan file: %w", err)
	}

	if err := os.WriteFile(path, append(encoded, '\n'), 0o644); err != nil { //nolint:gosec
		return fmt.Errorf("failed to write the plan file: %w", err)
	}

	return nil
}

// readPlanFile reads a plan file and refuses one this pista does not write.
func readPlanFile(path string) (*planFile, error) {
	encoded, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read the plan file: %w", err)
	}

	plan := &planFile{}
	if err := json.Unmarshal(encoded, plan); err != nil {
		return nil, fmt.Errorf("failed to parse the plan file: %w", err)
	}

	if plan.Version != planFileVersion {
		return nil, fmt.Errorf(
			"plan file %s is version %d, and this pista writes version %d: run plan again",
			path, plan.Version, planFileVersion,
		)
	}

	return plan, nil
}

// serverMajorVersion reads the major version of the server on the other end of
// the connection.
func serverMajorVersion(ctx context.Context, conn *pgx.Conn) (int, error) {
	var num int
	if err := conn.QueryRow(ctx, "SELECT current_setting('server_version_num')::int").Scan(&num); err != nil {
		return 0, fmt.Errorf("failed to read the server version: %w", err)
	}
	return num / 10000, nil
}
