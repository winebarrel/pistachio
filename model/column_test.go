package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/pistachio/model"
)

func TestColumnIdentity_IsIdentityColumn(t *testing.T) {
	assert.True(t, model.ColumnIdentity('a').IsIdentityColumn())
	assert.True(t, model.ColumnIdentity('d').IsIdentityColumn())
	assert.False(t, model.ColumnIdentity(0).IsIdentityColumn())
}

func TestColumnIdentity_IsGeneratedAlways(t *testing.T) {
	assert.True(t, model.ColumnIdentity('a').IsGeneratedAlways())
	assert.False(t, model.ColumnIdentity('d').IsGeneratedAlways())
}

func TestColumnIdentity_IsGeneratedByDefault(t *testing.T) {
	assert.True(t, model.ColumnIdentity('d').IsGeneratedByDefault())
	assert.False(t, model.ColumnIdentity('a').IsGeneratedByDefault())
}

func TestColumnGenerated_IsGeneratedColumn(t *testing.T) {
	assert.True(t, model.ColumnGenerated('s').IsGeneratedColumn())
	assert.True(t, model.ColumnGenerated('v').IsGeneratedColumn())
	assert.False(t, model.ColumnGenerated(0).IsGeneratedColumn())
}

func TestColumnGenerated_IsStoredGeneratedColumn(t *testing.T) {
	assert.True(t, model.ColumnGenerated('s').IsStoredGeneratedColumn())
	assert.False(t, model.ColumnGenerated('v').IsStoredGeneratedColumn())
}

func TestColumnGenerated_IsVirtualGeneratedColumn(t *testing.T) {
	assert.True(t, model.ColumnGenerated('v').IsVirtualGeneratedColumn())
	assert.False(t, model.ColumnGenerated('s').IsVirtualGeneratedColumn())
}

func TestColumn_String(t *testing.T) {
	col := &model.Column{Name: "id", TypeName: "integer", NotNull: true}
	s := col.String()
	assert.Contains(t, s, "id")
	assert.Contains(t, s, "integer")
}
