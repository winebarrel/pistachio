package pistachio

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/winebarrel/orderedmap/v2"
	"github.com/winebarrel/pistachio/catalog"
	"github.com/winebarrel/pistachio/model"
)

// stateHash fingerprints the current-side schema a plan was computed against,
// so apply-from can tell whether the database still is what the plan assumed.
//
// It covers what the diff read and nothing else. It is called on the objects
// handed to diff.Diff*: filtered, and with the storage parameters cleared
// where they are not managed. An object a filter left out cannot
// change the plan, so it cannot change this either, and an autovacuum setting
// nobody manages does not report drift.
//
// An object the desired schema marks -- pista:ignore is out of it too:
// removeIgnored drops it from the current side before the plan hashes it, and
// apply-from, which has no desired schema to find them from, drops the names
// the plan file records.
//
// Every object is hashed on its own and the digests are sorted, so the value
// does not depend on the order the catalog returned them in. An object list
// added to the plan file later can therefore name what drifted without the
// hash itself changing.
//
// The model carries each object's OID, so a table dropped and created again
// with the same definition is a different state, which is what the plan
// assumed it away from.
func (o *schemaObjects) stateHash() (string, error) {
	var digests []string

	// One call per kind, since each map is of its own type. They are listed
	// rather than called in sequence so that the failure of any of them is
	// reported in one place.
	for _, appendKind := range []func() error{
		func() error { return appendDigests(&digests, "table", o.Tables) },
		func() error { return appendDigests(&digests, "view", o.Views) },
		func() error { return appendDigests(&digests, "enum", o.Enums) },
		func() error { return appendDigests(&digests, "domain", o.Domains) },
		func() error { return appendDigests(&digests, "composite_type", o.CompositeTypes) },
		func() error { return appendDigests(&digests, "sequence", o.Sequences) },
		func() error { return appendDigests(&digests, "routine", o.Routines) },
	} {
		if err := appendKind(); err != nil {
			return "", err
		}
	}

	slices.Sort(digests)
	sum := sha256.Sum256([]byte(strings.Join(digests, "\n")))
	return hex.EncodeToString(sum[:]), nil
}

// appendDigests adds one digest per object of a kind. The kind and the key are
// hashed with the object so that two kinds cannot collide, and so that an
// object renamed into another's place is a different state even where the
// definition is identical.
//
// The JSON is the document pista parse and pista dump write, through the same
// marshalers, so one description of the model serves all three.
func appendDigests[V any](digests *[]string, kind string, objects *orderedmap.Map[string, V]) error {
	for key, object := range objects.All() {
		encoded, err := json.Marshal(object, json.Deterministic(true), model.JSONMarshalers)
		if err != nil {
			return fmt.Errorf("failed to encode %s %s: %w", kind, key, err)
		}
		sum := sha256.Sum256(fmt.Appendf(nil, "%s\x00%s\x00%s", kind, key, encoded))
		*digests = append(*digests, hex.EncodeToString(sum[:]))
	}
	return nil
}

// currentState reads the current schema and narrows it the way a plan run
// does. apply-from has no desired schema to diff against, only the statements
// the plan file already holds, so it reads the catalog for the state hash and
// the object count alone.
func (client *Client) currentState(ctx context.Context, conn *pgx.Conn) (*schemaObjects, error) {
	cat, err := catalog.NewCatalog(conn, client.Schemas)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog: %w", err)
	}

	current, err := readCurrent(ctx, cat, &client.FilterOptions)
	if err != nil {
		return nil, err
	}

	return client.currentSide(current), nil
}

// remove drops the named objects from every kind. The names are keys of the
// maps the catalog filled, so the kind a name belongs to does not have to be
// known: the six it does not belong to hold nothing under it.
func (o *schemaObjects) remove(names []string) {
	for _, name := range names {
		o.Tables.Delete(name)
		o.Views.Delete(name)
		o.Enums.Delete(name)
		o.Domains.Delete(name)
		o.CompositeTypes.Delete(name)
		o.Sequences.Delete(name)
		o.Routines.Delete(name)
	}
}

// currentSide narrows what the catalog returned to what the diff compares:
// the filters applied, and the storage parameters dropped where they are not
// managed. Both the plan and apply-from hash the result, so they have to
// narrow it the same way, which is why this is one function rather than two
// sets of calls that happen to agree today.
func (f *FilterOptions) currentSide(current *schemaObjects) *schemaObjects {
	narrowed := &schemaObjects{
		Tables:         f.filterTables(current.Tables),
		Views:          f.filterViews(current.Views),
		Enums:          f.filterEnums(current.Enums),
		Domains:        f.filterDomains(current.Domains),
		CompositeTypes: f.filterCompositeTypes(current.CompositeTypes),
		Sequences:      f.filterSequences(current.Sequences),
		Routines:       f.filterRoutines(current.Routines),
	}

	if !f.ManageStorageParam {
		clearStorageParams(narrowed.Tables)
		clearMatViewStorageParams(narrowed.Views)
	}

	return narrowed
}
