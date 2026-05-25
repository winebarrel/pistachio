package model_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/pistachio/model"
)

func TestConstraintType(t *testing.T) {
	assert.True(t, model.ConstraintType('c').IsCheckConstraint())
	assert.True(t, model.ConstraintType('f').IsForeignKeyConstraint())
	assert.True(t, model.ConstraintType('n').IsNotNullConstraint())
	assert.True(t, model.ConstraintType('p').IsPrimaryKeyConstraint())
	assert.True(t, model.ConstraintType('u').IsUniqueConstraint())
	assert.True(t, model.ConstraintType('t').IsConstraintTrigger())
	assert.True(t, model.ConstraintType('x').IsExclusionConstraint())

	assert.False(t, model.ConstraintType('c').IsPrimaryKeyConstraint())
	assert.False(t, model.ConstraintType('p').IsCheckConstraint())
}

func TestConstraint_String(t *testing.T) {
	con := &model.Constraint{Name: "pk", Type: model.ConstraintType('p'), Definition: "PRIMARY KEY (id)"}
	s := con.String()
	assert.Contains(t, s, "pk")
}

func TestForeignKey_String(t *testing.T) {
	fk := &model.ForeignKey{
		Constraint: model.Constraint{Name: "fk_user", Type: model.ConstraintType('f'), Definition: "FOREIGN KEY (user_id) REFERENCES users(id)"},
		Schema:     "public",
		Table:      "orders",
	}
	s := fk.String()
	assert.Contains(t, s, "fk_user")
}

func TestForeignKey_SQL(t *testing.T) {
	fk := model.ForeignKey{
		Constraint: model.Constraint{
			Name:       "fk_user",
			Type:       model.ConstraintType('f'),
			Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)",
			Validated:  true,
		},
		Schema: "public",
		Table:  "orders",
	}
	assert.Equal(t, "ALTER TABLE ONLY public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id);", fk.SQL())
}

func TestForeignKey_SQL_NotValid(t *testing.T) {
	fk := model.ForeignKey{
		Constraint: model.Constraint{
			Name:       "fk_user",
			Type:       model.ConstraintType('f'),
			Definition: "FOREIGN KEY (user_id) REFERENCES public.users(id)",
			Validated:  false,
		},
		Schema: "public",
		Table:  "orders",
	}
	assert.Equal(t, "ALTER TABLE ONLY public.orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES public.users(id) NOT VALID;", fk.SQL())
}
