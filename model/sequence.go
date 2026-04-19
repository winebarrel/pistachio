package model

import "fmt"

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
