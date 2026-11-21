package dsqlproxy

// tags puts back the command tag DSQL answers with for a statement the proxy
// rewrote: CREATE INDEX ASYNC answers CREATE INDEX on DSQL, where the
// rewritten SELECT answers SELECT 1.
//
// Each statement that runs gets one entry, in the order the client sent them,
// and each CommandComplete takes the first one. The entries of a simple query
// or of the extended messages up to a Sync form a batch, which the backend
// ends with ReadyForQuery. An error skips the rest of a batch, so
// ReadyForQuery drops what is left of it.
type tags struct {
	stmts   map[string]string // prepared statement -> tag
	portals map[string]string // portal -> tag
	batches [][]string
	open    bool // the last batch takes extended messages until a Sync
}

func newTags() *tags {
	return &tags{stmts: map[string]string{}, portals: map[string]string{}}
}

// query adds the batch of a simple query.
func (t *tags) query(stmtTags []string) {
	t.open = false
	t.batches = append(t.batches, stmtTags)
}

// parse records the tag of a prepared statement. An extended query holds one
// statement.
func (t *tags) parse(name string, stmtTags []string) {
	tag := ""
	if len(stmtTags) == 1 {
		tag = stmtTags[0]
	}
	t.stmts[name] = tag
}

func (t *tags) bind(portal, stmt string) {
	t.portals[portal] = t.stmts[stmt]
}

func (t *tags) execute(portal string) {
	if !t.open {
		t.batches = append(t.batches, nil)
		t.open = true
	}
	last := len(t.batches) - 1
	t.batches[last] = append(t.batches[last], t.portals[portal])
}

func (t *tags) sync() {
	t.open = false
}

// complete returns the tag for the next CommandComplete, or "" to keep the
// backend's.
func (t *tags) complete() string {
	if len(t.batches) == 0 || len(t.batches[0]) == 0 {
		return ""
	}
	tag := t.batches[0][0]
	t.batches[0] = t.batches[0][1:]
	return tag
}

// ready ends the first batch.
func (t *tags) ready() {
	if len(t.batches) > 0 {
		t.batches = t.batches[1:]
	}
}
