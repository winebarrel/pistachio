package parser_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/winebarrel/pistachio/parser"
)

func TestMakeObjectName_NotNull(t *testing.T) {
	assert.Equal(t, "items_id_not_null", parser.MakeObjectName("items", "id", "not_null"))
	// The table part is shortened first, so the label survives.
	assert.Equal(t, "customer_subscription_billing_history_arch_region_code_not_null",
		parser.MakeObjectName("customer_subscription_billing_history_archive_2024_snapshot", "region_code", "not_null"))
}
