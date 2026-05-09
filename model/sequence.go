package model

import "fmt"

// Sequence holds metadata for a PostgreSQL sequence. It is currently
// populated only by catalog.Catalog.Sequences and not yet consumed by the
// diff/apply pipeline; it is intentionally kept for future sequence-aware
// schema management and should not be deleted as dead code.
type Sequence struct {
	OID         uint32
	Schema      string
	Name        string
	DataType    string
	Start       int64
	Min         int64
	Max         int64
	Increment   int64
	Cache       int64
	Cycle       bool
	OwnerTable  *string
	OwnerColumn *string
}

func (seq *Sequence) String() string {
	return fmt.Sprintf("%#v", *seq)
}
