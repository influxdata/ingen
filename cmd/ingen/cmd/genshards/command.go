package genshards

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/ingen"
	"github.com/influxdata/ingen/pkg/gen"
	"github.com/spf13/cobra"
)

type command struct {
	PrintOnly               bool
	BuildTSI                bool
	Concurrency             int
	DataPath                string
	MetaPath                string
	StartTime               string
	Database                string
	RP                      string
	ShardCount              int
	ShardDuration           time.Duration
	Tags                    string
	PointsPerSeriesPerShard int
}

func New() *cobra.Command {
	var o command
	cmd := &cobra.Command{
		Use:   "gen-shards",
		Short: "Generate one or more fully-compacted shards",
		RunE:  o.Run,
	}

	fs := cmd.Flags()
	fs.BoolVar(&o.PrintOnly, "print", false, "Print data spec only")
	fs.BoolVar(&o.BuildTSI, "tsi", false, "Build TSI index")
	fs.IntVar(&o.Concurrency, "c", 1, "Concurrency")
	fs.StringVar(&o.DataPath, "data-path", "", "path to InfluxDB data")
	fs.StringVar(&o.MetaPath, "meta-path", "", "path to InfluxDB meta")
	fs.StringVar(&o.StartTime, "start-time", "", "Start time")
	fs.StringVar(&o.Database, "db", "db", "Name of database to create")
	fs.StringVar(&o.RP, "rp", "rp", "Name of retention policy")
	fs.IntVar(&o.ShardCount, "shards", 1, "Number of shards to create")
	fs.DurationVar(&o.ShardDuration, "shard-duration", 24*time.Hour, "Shard duration (default 24h)")
	fs.StringVar(&o.Tags, "t", "10,10,10", "Tag cardinality")
	fs.IntVar(&o.PointsPerSeriesPerShard, "p", 100, "Points per series per shard")



	return cmd
}

func (cmd *command) Run(_ *cobra.Command, args []string) error {
	db, gens, err := cmd.processOptions()
	if err != nil {
		return err
	}

	if db == nil {
		return nil
	}

	// Report stats.
	start := time.Now().UTC()
	defer func() {
		elapsed := time.Since(start)
		fmt.Println()
		fmt.Printf("Total time: %0.1f seconds\n", elapsed.Seconds())
	}()

	groups := db.Info.RetentionPolicy(db.Info.DefaultRetentionPolicy).ShardGroups

	g := ingen.Generator{Concurrency: cmd.Concurrency, BuildTSI: cmd.BuildTSI}
	return g.Run(context.Background(), db.database, db.ShardPath, groups, gens)
}

func (cmd *command) processOptions() (db *Database, gens []ingen.SeriesGenerator, err error) {
	cfg := new(DBConfig)

	cfg.Database = cmd.Database
	cfg.RP = cmd.RP
	cfg.DataPath = cmd.DataPath
	cfg.MetaPath = cmd.MetaPath
	cfg.ShardDuration.Duration = cmd.ShardDuration
	cfg.ShardCount = cmd.ShardCount

	if cmd.StartTime != "" {
		if t, err := time.Parse(time.RFC3339, cmd.StartTime); err != nil {
			return nil, nil, err
		} else {
			cfg.StartTime = t.UTC()
		}
	}

	if err = cfg.Validate(); err != nil {
		return nil, nil, err
	}

	// Parse tag cardinalities.
	var (
		tags  []int
		tagsN int
	)
	tagsN = 1
	for _, s := range strings.Split(cmd.Tags, ",") {
		v, err := strconv.Atoi(s)
		if err != nil {
			return nil, nil, fmt.Errorf("cannot parse tag cardinality: %s", s)
		}
		tags = append(tags, v)
		tagsN *= v
	}

	fmt.Fprintf(os.Stdout, "Data Path: %s\n", cfg.DataPath)
	fmt.Fprintf(os.Stdout, "Meta Path: %s\n", cfg.MetaPath)
	fmt.Fprintf(os.Stdout, "Concurrency: %d\n", cmd.Concurrency)
	fmt.Fprintf(os.Stdout, "Tag cardinalities: %+v\n", tags)
	fmt.Fprintf(os.Stdout, "Points per series per shard: %d\n", cmd.PointsPerSeriesPerShard)
	fmt.Fprintf(os.Stdout, "Total points per shard: %d\n", tagsN*cmd.PointsPerSeriesPerShard)
	fmt.Fprintf(os.Stdout, "Total series: %d\n", tagsN)
	fmt.Fprintf(os.Stdout, "Total points: %d\n", tagsN*cfg.ShardCount*cmd.PointsPerSeriesPerShard)
	fmt.Fprintf(os.Stdout, "Shard Count: %d\n", cfg.ShardCount)
	fmt.Fprintf(os.Stdout, "Database: %s/%s (Shard duration: %s)\n", cfg.Database, cfg.RP, cfg.ShardDuration)
	fmt.Fprintf(os.Stdout, "TSI: %t\n", cmd.BuildTSI)
	fmt.Fprintf(os.Stdout, "Start time: %s\n", cfg.StartTime)
	fmt.Fprintf(os.Stdout, "End time: %s\n", cfg.EndTime())
	fmt.Fprintln(os.Stdout)

	if cmd.PrintOnly {
		return nil, nil, nil
	}

	db = NewDatabase(cfg)
	if err = db.Create(); err != nil {
		return nil, nil, err
	}

	groups := db.Info.RetentionPolicy(db.Info.DefaultRetentionPolicy).ShardGroups
	gens = make([]ingen.SeriesGenerator, len(groups))
	for i := range gens {
		var (
			name []byte
			keys []string
			tv   []gen.Sequence
		)

		name = []byte("m0")
		tv = make([]gen.Sequence, len(tags))
		setTagVals(tags, tv)
		keys = make([]string, len(tags))
		setTagKeys("tag", keys)

		sgi := &groups[i]
		vg := gen.NewIntegerConstantValuesSequence(cmd.PointsPerSeriesPerShard, sgi.StartTime, cfg.ShardDuration.Duration/time.Duration(cmd.PointsPerSeriesPerShard), 1)

		gens[i] = gen.NewSeriesGenerator(name, "v0", vg, gen.NewTagsValuesSequenceKeysValues(keys, tv))
	}

	return db, gens, nil

}

func setTagVals(tags []int, tv []gen.Sequence) {
	for j := range tags {
		tv[j] = gen.NewCounterByteSequenceCount(tags[j])
	}
}

func setTagKeys(prefix string, keys []string) {
	tw := int(math.Ceil(math.Log10(float64(len(keys)))))
	tf := fmt.Sprintf("%s%%0%dd", prefix, tw)
	for i := range keys {
		keys[i] = fmt.Sprintf(tf, i)
	}
}
