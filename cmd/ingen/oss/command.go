package oss

import (
	"context"
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/ingen"
	"github.com/influxdata/ingen/cmd/ingen/gen"
)

type Command struct {
	PrintOnly               bool
	BuildTSI                bool
	Concurrency             int
	DataPath                string
	MetaPath                string
	StartTime               string
	OrgID                   string
	Database                string
	RP                      string
	ShardCount              int
	ShardDuration           time.Duration
	Tags                    string
	PointsPerSeriesPerShard int
}

func New() *Command { return new(Command) }

func (cmd *Command) Run(args []string) error {
	db, gens, err := cmd.parseFlags(args)
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

func (cmd *Command) parseFlags(args []string) (db *Database, gens []ingen.SeriesGenerator, err error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.BoolVar(&cmd.PrintOnly, "print", false, "Print data spec only")
	fs.BoolVar(&cmd.BuildTSI, "tsi", false, "Build TSI index")
	fs.IntVar(&cmd.Concurrency, "c", 1, "Concurrency")
	fs.StringVar(&cmd.DataPath, "data-path", "", "path to InfluxDB data")
	fs.StringVar(&cmd.MetaPath, "meta-path", "", "path to InfluxDB meta")
	fs.StringVar(&cmd.StartTime, "start-time", "", "Start time (default now - (shards * shard-duration))")
	fs.StringVar(&cmd.OrgID, "org-id", "", "Optional: Specify org-id to create multi-tenant data")
	fs.StringVar(&cmd.Database, "db", "db", "Database (single tenant) or bucket id (multi-tenant) to create")
	fs.StringVar(&cmd.RP, "rp", "rp", "Default retention policy")
	fs.IntVar(&cmd.ShardCount, "shards", 1, "Number of shards to create")
	fs.DurationVar(&cmd.ShardDuration, "shard-duration", 24*time.Hour, "Shard duration (default 24h)")
	fs.StringVar(&cmd.Tags, "t", "10,10,10", "Tag cardinality")
	fs.IntVar(&cmd.PointsPerSeriesPerShard, "p", 100, "Points per series per shard")

	if err := fs.Parse(args); err != nil {
		return nil, nil, err
	}

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

	if cmd.OrgID != "" {
		cfg.Database = "db"
		cfg.RP = "rp"
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
	if cmd.OrgID != "" {
		fmt.Fprintf(os.Stdout, "Tenant+Bucket: %s+%s\n", cmd.OrgID, cmd.Database)
	}
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

		if cmd.OrgID != "" {
			name = []byte(cmd.OrgID)
			name = append(name, 0, 0)
			name = append(name, cmd.Database...)

			tv = make([]gen.Sequence, len(tags)+1)
			tv[0] = gen.ConstantStringSequence("m0")
			setTagVals(tags, tv[1:])

			keys = make([]string, len(tv))
			keys[0] = "_m"
			setTagKeys("tag", keys[1:])
		} else {
			name = []byte("m0")
			tv = make([]gen.Sequence, len(tags))
			setTagVals(tags, tv)
			keys = make([]string, len(tags))
			setTagKeys("tag", keys)
		}

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
