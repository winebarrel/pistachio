package pistachio

import (
	"github.com/winebarrel/orderedmap"
	"github.com/winebarrel/pistachio/model"
)

func (client *Client) filterTables(tables *orderedmap.Map[string, *model.Table]) *orderedmap.Map[string, *model.Table] {
	if len(client.Include) == 0 && len(client.Exclude) == 0 {
		return tables
	}

	filtered := orderedmap.New[string, *model.Table]()
	for k, t := range tables.All() {
		if client.MatchName(t.Name) {
			filtered.Set(k, t)
		}
	}
	return filtered
}

func (client *Client) filterViews(views *orderedmap.Map[string, *model.View]) *orderedmap.Map[string, *model.View] {
	if len(client.Include) == 0 && len(client.Exclude) == 0 {
		return views
	}

	filtered := orderedmap.New[string, *model.View]()
	for k, v := range views.All() {
		if client.MatchName(v.Name) {
			filtered.Set(k, v)
		}
	}
	return filtered
}
