package cloud

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
	Concurrency             int
	DataPath                string
	StartTime               string
	Duration                time.Duration
	OrgID                   string
	BucketID                string
	ShardCount              int
	NodeCount               int
	Tags                    string
	PointsPerSeriesPerShard int
}

func New() *Command { return new(Command) }

func (cmd *Command) Run(args []string) error {
	db, gens, err := cmd.parseFlags(args)
	if err != nil {
		os.Exit(1)
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

	g := ingen.Generator{Concurrency: cmd.Concurrency}
	return g.Run(context.Background(), db.database, db.ShardPath, groups, gens)
}

func (cmd *Command) parseFlags(args []string) (db *Database, gens []ingen.SeriesGenerator, err error) {
	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.BoolVar(&cmd.PrintOnly, "print", false, "Print data spec only")
	fs.IntVar(&cmd.Concurrency, "c", 1, "Concurrency")
	fs.StringVar(&cmd.DataPath, "data-path", "", "path to storage data")
	fs.StringVar(&cmd.StartTime, "start-time", "", "Start time (default now - (shards * shard-duration))")
	fs.DurationVar(&cmd.Duration, "duration", 24*time.Hour, "Duration to spread data over (default 24h)")
	fs.StringVar(&cmd.OrgID, "org-id", "", "Optional: Specify org-id to create multi-tenant data")
	fs.StringVar(&cmd.BucketID, "bucket-id", "", "Bucket id to create")
	fs.IntVar(&cmd.ShardCount, "shards", 1, "Number of shards to create")
	fs.IntVar(&cmd.NodeCount, "shards", 1, "Number of nodes to create")
	fs.StringVar(&cmd.Tags, "t", "10,10,10", "Tag cardinality")
	fs.IntVar(&cmd.PointsPerSeriesPerShard, "p", 100, "Points per series per shard")

	if err := fs.Parse(args); err != nil {
		return nil, nil, err
	}

	cfg := new(DBConfig)

	cfg.DataPath = cmd.DataPath
	cfg.ShardCount = cmd.ShardCount
	cfg.NodeCount = cmd.NodeCount

	var startTime time.Time
	if cmd.StartTime != "" {
		if t, err := time.Parse(time.RFC3339, cmd.StartTime); err != nil {
			return nil, nil, err
		} else {
			startTime = t.UTC()
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
	fmt.Fprintf(os.Stdout, "Concurrency: %d\n", cmd.Concurrency)
	fmt.Fprintf(os.Stdout, "Tag cardinalities: %+v\n", tags)
	fmt.Fprintf(os.Stdout, "Points per series per shard: %d\n", cmd.PointsPerSeriesPerShard)
	fmt.Fprintf(os.Stdout, "Total points per shard: %d\n", tagsN*cmd.PointsPerSeriesPerShard)
	fmt.Fprintf(os.Stdout, "Total series: %d\n", tagsN)
	fmt.Fprintf(os.Stdout, "Total points: %d\n", tagsN*cfg.ShardCount*cmd.PointsPerSeriesPerShard)
	fmt.Fprintf(os.Stdout, "Shard Count: %d\n", cfg.ShardCount)
	if cmd.OrgID != "" {
		fmt.Fprintf(os.Stdout, "Tenant+Bucket: %s+%s\n", cmd.OrgID, cmd.BucketID)
	}
	fmt.Fprintf(os.Stdout, "Start time: %s\n", startTime)
	fmt.Fprintf(os.Stdout, "End time: %s\n", startTime.Add(cmd.Duration))
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

		name = []byte(cmd.OrgID)
		name = append(name, 0, 0)
		name = append(name, cmd.BucketID...)

		tv = make([]gen.Sequence, len(tags)+1)
		tv[0] = gen.ConstantStringSequence("m0")
		setTagVals(tags, tv[1:])

		keys = make([]string, len(tv))
		keys[0] = "_m"
		setTagKeys("tag", keys[1:])

		vg := gen.NewIntegerConstantValuesSequence(cmd.PointsPerSeriesPerShard, startTime, cmd.Duration/time.Duration(cmd.PointsPerSeriesPerShard), 1)

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
