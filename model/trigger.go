package model

import "fmt"

// TriggerState mirrors pg_trigger.tgenabled:
//
//	'O' fires in origin and local sessions, which is where CREATE TRIGGER
//	leaves a new trigger, 'D' never fires, 'R' fires in replica mode only,
//	and 'A' fires in every mode.
type TriggerState byte

// TriggerStateDefault is the state CREATE TRIGGER leaves behind.
const TriggerStateDefault TriggerState = 'O'

// IsDefault reports the state a bare CREATE TRIGGER produces. The zero value
// counts, so a trigger built without a state reads as enabled.
func (s TriggerState) IsDefault() bool {
	return s == 0 || s == TriggerStateDefault
}

// Action returns the ALTER TABLE action that puts a trigger in the state.
func (s TriggerState) Action() string {
	switch s {
	case 'D':
		return "DISABLE"
	case 'R':
		return "ENABLE REPLICA"
	case 'A':
		return "ENABLE ALWAYS"
	default:
		return "ENABLE"
	}
}

type Trigger struct {
	Schema     string
	Table      string
	Name       string
	RenameFrom *string
	// Definition is the whole CREATE TRIGGER statement without its
	// terminator: what pg_get_triggerdef writes on the catalog side, and what
	// pg_query deparses on the desired side. The two renderings differ in
	// places, so the diff cannot compare this text directly; see
	// diff.equalTriggerDef.
	Definition string
	State      TriggerState
}

func (trg *Trigger) String() string {
	return fmt.Sprintf("%#v", *trg)
}

// FQTN returns the qualified name of the table or view the trigger is on.
func (trg Trigger) FQTN() string {
	return Ident(trg.Schema, trg.Table)
}

func (trg Trigger) SQL() string {
	return trg.Definition + ";"
}

// StateSQL renders the ALTER TABLE that leaves the trigger in a non-default
// state, and "" for the default one, which CREATE TRIGGER already produces.
func (trg Trigger) StateSQL() string {
	if trg.State.IsDefault() {
		return ""
	}
	return TriggerStateSQL(trg.FQTN(), trg.Name, trg.State)
}

// TriggerStateSQL renders the ALTER TABLE that puts a trigger in the given
// state. Exported so diff can reuse it for the case StateSQL suppresses: a
// trigger going back to the default state after CREATE OR REPLACE TRIGGER
// reset it, which still needs the statement CREATE TRIGGER alone would not.
func TriggerStateSQL(fqtn, name string, state TriggerState) string {
	return "ALTER TABLE " + fqtn + " " + state.Action() + " TRIGGER " + Ident(name) + ";"
}
