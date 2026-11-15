package pistachio

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// errCheckNull is a check that returned NULL. A NULL can depend on the state
// of the database, so plan leaves the decision to apply.
var errCheckNull = errors.New("check SQL returned NULL")

// checkNotBooleanError is a check that returned a value other than a boolean.
// The type does not depend on the state of the database, so plan refuses it.
type checkNotBooleanError struct {
	value any
}

func (e *checkNotBooleanError) Error() string {
	// %#v quotes a string, so a text 'true' does not read as a boolean.
	return fmt.Sprintf("check SQL must return a boolean, got %#v", e.value)
}

// evalCheck reads the result of the check SQL of a -- pista:execute directive.
func evalCheck(row pgx.Row) (bool, error) {
	var v any
	if err := row.Scan(&v); err != nil {
		return false, err
	}
	switch b := v.(type) {
	case bool:
		return b, nil
	case nil:
		return false, errCheckNull
	default:
		return false, &checkNotBooleanError{value: v}
	}
}
