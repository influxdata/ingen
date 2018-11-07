package gen

import (
	"math/rand"
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

type FloatConstantValuesSequence struct {
	buf   tsm1.Values
	vals  tsm1.Values
	n     int
	t     int64
	state struct {
		n int
		t int64
		d int64
		v float64
	}
}

func NewFloatConstantValuesSequence(n int, start time.Time, delta time.Duration, v float64) *FloatConstantValuesSequence {
	g := &FloatConstantValuesSequence{buf: make(tsm1.Values, tsdb.DefaultMaxPointsPerBlock)}
	g.state.n = n
	g.state.t = start.UnixNano()
	g.state.d = int64(delta)
	g.state.v = v
	g.Reset()
	return g
}

func (g *FloatConstantValuesSequence) Reset() {
	g.n = g.state.n
	g.t = g.state.t
}

func (g *FloatConstantValuesSequence) Next() bool {
	if g.n == 0 {
		return false
	}

	c := min(g.n, tsdb.DefaultMaxPointsPerBlock)
	g.n -= c
	g.vals = g.buf[:c]

	for i := range g.vals {
		g.vals[i] = tsm1.NewFloatValue(g.t, g.state.v)
		g.t += g.state.d
	}
	return true
}

func (g *FloatConstantValuesSequence) Values() tsm1.Values { return g.vals }

type FloatRandomValuesSequence struct {
	buf   tsm1.Values
	vals  tsm1.Values
	n     int
	t     int64
	state struct {
		n     int
		t     int64
		d     int64
		scale float64
	}
}

func NewFloatRandomValuesSequence(n int, start time.Time, delta time.Duration, scale float64) *FloatRandomValuesSequence {
	g := &FloatRandomValuesSequence{buf: make(tsm1.Values, tsdb.DefaultMaxPointsPerBlock)}
	g.state.n = n
	g.state.t = start.UnixNano()
	g.state.d = int64(delta)
	g.state.scale = scale
	g.Reset()
	return g
}

func (g *FloatRandomValuesSequence) Reset() {
	g.n = g.state.n
	g.t = g.state.t
}

func (g *FloatRandomValuesSequence) Next() bool {
	if g.n == 0 {
		return false
	}

	c := min(g.n, tsdb.DefaultMaxPointsPerBlock)
	g.n -= c
	g.vals = g.buf[:c]
	for i := range g.vals {
		g.vals[i] = tsm1.NewFloatValue(g.t, rand.Float64()*g.state.scale)
		g.t += g.state.d
	}
	return true
}

func (g *FloatRandomValuesSequence) Values() tsm1.Values { return g.vals }

type IntegerRandomValuesSequence struct {
	buf   tsm1.Values
	vals  tsm1.Values
	n     int
	t     int64
	state struct {
		n   int
		t   int64
		d   int64
		max int64
	}
}

func NewIntegerRandomValuesSequence(n int, start time.Time, delta time.Duration, max int64) *IntegerRandomValuesSequence {
	g := &IntegerRandomValuesSequence{buf: make(tsm1.Values, tsdb.DefaultMaxPointsPerBlock)}
	g.state.n = n
	g.state.t = start.UnixNano()
	g.state.d = int64(delta)
	g.state.max = max
	g.Reset()
	return g
}

func (g *IntegerRandomValuesSequence) Reset() {
	g.n = g.state.n
	g.t = g.state.t
}

func (g *IntegerRandomValuesSequence) Next() bool {
	if g.n == 0 {
		return false
	}

	c := min(g.n, tsdb.DefaultMaxPointsPerBlock)
	g.n -= c
	g.vals = g.buf[:c]
	for i := range g.vals {
		g.vals[i] = tsm1.NewIntegerValue(g.t, rand.Int63n(g.state.max))
		g.t += g.state.d
	}
	return true
}

func (g *IntegerRandomValuesSequence) Values() tsm1.Values { return g.vals }