package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConstraintType(t *testing.T) {
	assert.True(t, ConstraintType('c').IsCheckConstraint())
	assert.True(t, ConstraintType('f').IsForeignKeyConstraint())
	assert.True(t, ConstraintType('n').IsNotNullConstraint())
	assert.True(t, ConstraintType('p').IsPrimaryKeyConstraint())
	assert.True(t, ConstraintType('u').IsUniqueConstraint())
	assert.True(t, ConstraintType('t').IsConstraintTrigger())
	assert.True(t, ConstraintType('x').IsExclusionConstraint())

	assert.False(t, ConstraintType('c').IsPrimaryKeyConstraint())
	assert.False(t, ConstraintType('p').IsCheckConstraint())
}

func TestConstraint_String(t *testing.T) {
	con := &Constraint{Name: "pk", Type: ConstraintType('p'), Definition: "PRIMARY KEY (id)"}
	s := con.String()
	assert.Contains(t, s, "pk")
}

func TestForeignKey_String(t *testing.T) {
	fk := &ForeignKey{
		Constraint: Constraint{Name: "fk_user", Type: ConstraintType('f'), Definition: "FOREIGN KEY (user_id) REFERENCES users(id)"},
		Schema:     "public",
		Table:      "orders",
	}
	s := fk.String()
	assert.Contains(t, s, "fk_user")
}

func TestForeignKey_SQL(t *testing.T) {
	fk := ForeignKey{
		Constraint: Constraint{
			Name:       "fk_user",
			Type:       ConstraintType('f'),
			Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)",
			Validated:  true,
		},
		Schema: "public",
		Table:  "orders",
	}
	assert.Equal(t, "ALTER TABLE ONLY public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id);", fk.SQL())
}

func TestForeignKey_SQL_NotValid(t *testing.T) {
	fk := ForeignKey{
		Constraint: Constraint{
			Name:       "fk_user",
			Type:       ConstraintType('f'),
			Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)",
			Validated:  false,
		},
		Schema: "public",
		Table:  "orders",
	}
	assert.Equal(t, "ALTER TABLE ONLY public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id) NOT VALID;", fk.SQL())
}
