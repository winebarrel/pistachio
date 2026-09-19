package parser

// undeclared reports stmt, which names a kind and name no statement before
// offset declares: a typo, or a file that comes too early. The parser reads
// the input in one pass, as PostgreSQL would run it, so the declaration has to
// come first. The error carries the statement's position.
func undeclared(stmt, kind, name string, offset int32) error {
	return &locatedError{msg: stmt + ": " + kind + " " + name + " is not declared before it", offset: int(offset)}
}
