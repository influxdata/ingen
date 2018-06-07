package gen

import (
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
	"github.com/influxdata/ingen"
)

type SeriesGenerator struct {
	name  []byte
	tags  TagsSequence
	field string
	vg    ingen.ValuesSequence
	buf   []byte
}

func NewSeriesGenerator(name []byte, field string, vg ingen.ValuesSequence, tags TagsSequence) *SeriesGenerator {
	return &SeriesGenerator{
		name:  name,
		field: field,
		vg:    vg,
		tags:  tags,
	}
}

func (g *SeriesGenerator) Next() bool {
	if !g.tags.Next() {
		return false
	}

	g.vg.Reset()
	g.buf = models.AppendMakeKey(g.buf[:0], g.name, g.tags.Value())

	return true
}

func (g *SeriesGenerator) Key() []byte                           { return tsm1.SeriesFieldKeyBytes(string(g.buf), g.field) }
func (g *SeriesGenerator) ValuesGenerator() ingen.ValuesSequence { return g.vg }
