package ingen

import (
	"context"
	"fmt"
	"sync"

	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

type SeriesGenerator interface {
	Next() bool
	Key() []byte
	ValuesGenerator() ValuesSequence
}

type ValuesSequence interface {
	Reset()
	Next() bool
	Values() tsm1.Values
}

type Generator struct {
	Concurrency int
}

func (g *Generator) Run(ctx context.Context, db *Database, gens []SeriesGenerator) (err error) {
	groups := db.Info.RetentionPolicy(db.Info.DefaultRetentionPolicy).ShardGroups

	limit := make(chan struct{}, g.Concurrency)
	for i := 0; i < g.Concurrency; i++ {
		limit <- struct{}{}
	}

	var (
		wg   sync.WaitGroup
		errs ErrorList
		ch   = make(chan error, len(groups))
	)

	wg.Add(len(groups))
	for i := 0; i < len(groups); i++ {
		go func(n int) {
			<-limit
			defer func() {
				wg.Done()
				limit <- struct{}{}
			}()

			id := groups[n].ID
			if err = g.writeShard(gens[n], id, db.ShardPath); err != nil {
				ch <- fmt.Errorf("error writing shard %d: %s", id, err.Error())
			}
		}(i)
	}
	wg.Wait()

	close(ch)
	for e := range ch {
		errs = append(errs, e)
	}

	return errs
}

func (g *Generator) writeShard(sg SeriesGenerator, id uint64, path string) error {
	sw := newShardWriter(id, path)
	defer sw.Close()

	for sg.Next() {
		key := sg.Key()
		vg := sg.ValuesGenerator()

		for vg.Next() {
			sw.Write(key, vg.Values())
		}

		if err := sw.Err(); err != nil {
			return err
		}
	}
	return nil
}
