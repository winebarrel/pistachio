package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestColumnIdentity_IsIdentityColumn(t *testing.T) {
	assert.True(t, ColumnIdentity('a').IsIdentityColumn())
	assert.True(t, ColumnIdentity('d').IsIdentityColumn())
	assert.False(t, ColumnIdentity(0).IsIdentityColumn())
}

func TestColumnIdentity_IsGeneratedAlways(t *testing.T) {
	assert.True(t, ColumnIdentity('a').IsGeneratedAlways())
	assert.False(t, ColumnIdentity('d').IsGeneratedAlways())
}

func TestColumnIdentity_IsGeneratedByDefault(t *testing.T) {
	assert.True(t, ColumnIdentity('d').IsGeneratedByDefault())
	assert.False(t, ColumnIdentity('a').IsGeneratedByDefault())
}

func TestColumnGenerated_IsGeneratedColumn(t *testing.T) {
	assert.True(t, ColumnGenerated('s').IsGeneratedColumn())
	assert.True(t, ColumnGenerated('v').IsGeneratedColumn())
	assert.False(t, ColumnGenerated(0).IsGeneratedColumn())
}

func TestColumnGenerated_IsStoredGeneratedColumn(t *testing.T) {
	assert.True(t, ColumnGenerated('s').IsStoredGeneratedColumn())
	assert.False(t, ColumnGenerated('v').IsStoredGeneratedColumn())
}

func TestColumnGenerated_IsVirtualGeneratedColumn(t *testing.T) {
	assert.True(t, ColumnGenerated('v').IsVirtualGeneratedColumn())
	assert.False(t, ColumnGenerated('s').IsVirtualGeneratedColumn())
}

func TestColumn_String(t *testing.T) {
	col := &Column{Name: "id", TypeName: "integer", NotNull: true}
	s := col.String()
	assert.Contains(t, s, "id")
	assert.Contains(t, s, "integer")
}
