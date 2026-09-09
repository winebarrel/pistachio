package pistachio

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSizePretty(t *testing.T) {
	tests := []struct {
		bytes int64
		want  string
	}{
		{0, "0 bytes"},
		{8192, "8192 bytes"},
		{10239, "10239 bytes"},
		{10240, "10 kB"},
		{16384, "16 kB"},
		{10485760, "10 MB"},
		{10096869376, "9629 MB"},
		{1 << 40, "1024 GB"},
		{1 << 50, "1024 TB"},
		{1 << 60, "1024 PB"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, sizePretty(tt.bytes), "%d", tt.bytes)
	}
}

func TestGroupDigits(t *testing.T) {
	tests := []struct {
		n    int64
		want string
	}{
		{0, "0"},
		{3, "3"},
		{999, "999"},
		{1000, "1,000"},
		{12345, "12,345"},
		{123456, "123,456"},
		{1234567, "1,234,567"},
		{120000000, "120,000,000"},
	}
	for _, tt := range tests {
		assert.Equal(t, tt.want, groupDigits(tt.n), "%d", tt.n)
	}
}

func TestTypmodOf(t *testing.T) {
	assert.Nil(t, typmodOf("text"))
	assert.Equal(t, []int64{50}, typmodOf("character varying(50)"))
	assert.Equal(t, []int64{10, 2}, typmodOf("numeric(10,2)"))
	assert.Equal(t, []int64{3}, typmodOf("timestamp(3) without time zone"))
	assert.Equal(t, []int64{50}, typmodOf("character varying(50)[]"))
}

func TestBaseTypeString(t *testing.T) {
	assert.Equal(t, "text", baseTypeString("text"))
	assert.Equal(t, "character varying", baseTypeString("character varying(50)"))
	assert.Equal(t, "timestamp without time zone", baseTypeString("timestamp(3) without time zone"))
	assert.Equal(t, "numeric", baseTypeString("numeric(10,2)[]"))
}

func TestTypmodWidens(t *testing.T) {
	tests := []struct {
		name     string
		baseType string
		src, dst []int64
		want     bool
	}{
		{"any type drops the modifier", "character", []int64{5}, nil, true},
		{"varchar wider", "character varying", []int64{50}, []int64{100}, true},
		{"varchar narrower", "character varying", []int64{50}, []int64{20}, false},
		{"varchar from unbounded", "character varying", nil, []int64{20}, false},
		{"varbit wider", "bit varying", []int64{8}, []int64{16}, true},
		{"numeric more precision", "numeric", []int64{10, 2}, []int64{12, 2}, true},
		{"numeric other scale", "numeric", []int64{12, 2}, []int64{12, 3}, false},
		{"numeric less precision", "numeric", []int64{12, 2}, []int64{10, 2}, false},
		{"numeric precision alone is scale zero", "numeric", []int64{5, 0}, []int64{7}, true},
		{"numeric from unbounded", "numeric", nil, []int64{10, 2}, false},
		{"timestamp more precision", "timestamp without time zone", []int64{3}, []int64{6}, true},
		{"timestamp less precision", "timestamp with time zone", []int64{6}, []int64{3}, false},
		{"timestamp from default precision", "time without time zone", nil, []int64{6}, true},
		{"time below default precision", "time with time zone", nil, []int64{3}, false},
		{"character never", "character", []int64{5}, []int64{10}, false},
		{"bit never", "bit", []int64{5}, []int64{10}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, typmodWidens(tt.baseType, tt.src, tt.dst))
		})
	}
}

func TestExplainEffect_Merge(t *testing.T) {
	eff := explainEffect{touch: touchScan, block: blockWrites, targets: []explainTarget{{key: "public.a", rebuilt: 1}}}
	eff.merge(explainEffect{touch: touchRewrite, block: blockNothing, targets: []explainTarget{{key: "public.a", rebuilt: 3}, {key: "public.b"}}})
	assert.Equal(t, touchRewrite, eff.touch)
	assert.Equal(t, blockWrites, eff.block)
	assert.Equal(t, []explainTarget{{key: "public.a", rebuilt: 3}, {key: "public.b"}}, eff.targets)
}

func TestParseOneStmt(t *testing.T) {
	assert.NotNil(t, parseOneStmt("ALTER TABLE public.t ADD COLUMN a integer;"))
	assert.Nil(t, parseOneStmt("-- a comment alone"))
	assert.Nil(t, parseOneStmt("ALTER TABLE public.t ADD COLUMN a integer; DROP TABLE public.u;"))
	assert.Nil(t, parseOneStmt("not sql"))
}
