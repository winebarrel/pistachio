package model

import (
	"encoding/json/jsontext"
	"encoding/json/v2"

	"github.com/winebarrel/orderedmap/v2"
)

// JSONMarshalers holds what the JSON document pistachio writes needs on top of
// the struct tags. `parse` and `dump` both write through it, and the JSON
// Schema describes what it produces, so a test that takes a golden through it
// sees the bytes the commands write.
var JSONMarshalers = json.WithMarshalers(json.MarshalToFunc(marshalColumns))

// marshalColumns writes a table's columns as a JSON array rather than an
// object keyed by name. The order is the physical column order, which a JSON
// object does not promise to keep, and the key an object would carry is
// already the column's name field. CollectValues holds the order and answers
// an empty table with an empty slice, which is written as [] rather than null.
func marshalColumns(enc *jsontext.Encoder, columns *orderedmap.Map[string, *Column]) error {
	return json.MarshalEncode(enc, columns.CollectValues())
}
