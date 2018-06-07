package gen

import (
	"time"

	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type IntegerConstantValuesSequence struct {
	buf   tsm1.Values
	vals  tsm1.Values
	n     int
	t     int64
	state struct {
		n int
		t int64
		d int64
		v int64
	}
}

func NewIntegerConstantValuesSequence(n int, start time.Time, delta time.Duration, v int64) *IntegerConstantValuesSequence {
	g := &IntegerConstantValuesSequence{buf: make(tsm1.Values, tsdb.DefaultMaxPointsPerBlock)}
	g.state.n = n
	g.state.t = start.UnixNano()
	g.state.d = int64(delta)
	g.state.v = v
	g.Reset()
	return g
}

func (g *IntegerConstantValuesSequence) Reset() {
	g.n = g.state.n
	g.t = g.state.t
}

func (g *IntegerConstantValuesSequence) Next() bool {
	if g.n == 0 {
		return false
	}

	c := min(g.n, tsdb.DefaultMaxPointsPerBlock)
	g.n -= c
	g.vals = g.buf[:c]

	for i := range g.vals {
		g.vals[i] = tsm1.NewIntegerValue(g.t, g.state.v)
		g.t += g.state.d
	}
	return true
}

func (g *IntegerConstantValuesSequence) Values() tsm1.Values { return g.vals }
