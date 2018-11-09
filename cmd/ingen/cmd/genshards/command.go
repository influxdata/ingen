package genshards

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/influxdata/ingen"
	"github.com/influxdata/ingen/pkg/gen"
	"github.com/spf13/cobra"
	"golang.org/x/text/message"
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
	Fields                  int
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
	fs.IntVar(&o.Fields, "f", 1, "Fields per point")

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

func (cmd *command) processOptions() (db *Database, gens [][]ingen.SeriesGenerator, err error) {
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

	mp := message.NewPrinter(message.MatchLanguage("en"))
	tw := tabwriter.NewWriter(os.Stdout, 25, 4, 2, ' ', 0)
	mp.Fprintf(tw, "Data Path\t%s\n", cfg.DataPath)
	mp.Fprintf(tw, "Meta Path\t%s\n", cfg.MetaPath)
	mp.Fprintf(tw, "Concurrency\t%d\n", cmd.Concurrency)
	mp.Fprintf(tw, "Tag cardinalities\t%s\n", fmt.Sprintf("%+v", tags))
	mp.Fprintf(tw, "Points per series per shard\t%d\n", cmd.PointsPerSeriesPerShard)
	mp.Fprintf(tw, "Fields per point\t%d\n", cmd.Fields)
	mp.Fprintf(tw, "Total points per shard\t%d\n", tagsN*cmd.PointsPerSeriesPerShard)
	mp.Fprintf(tw, "Total series\t%d\n", tagsN)
	mp.Fprintf(tw, "Total points\t%d\n", tagsN*cfg.ShardCount*cmd.PointsPerSeriesPerShard)
	mp.Fprintf(tw, "Shard Count\t%d\n", cfg.ShardCount)
	mp.Fprintf(tw, "Database\t%s/%s (Shard duration: %s)\n", cfg.Database, cfg.RP, cfg.ShardDuration)
	mp.Fprintf(tw, "TSI\t%t\n", cmd.BuildTSI)
	mp.Fprintf(tw, "Start time\t%s\n", cfg.StartTime)
	mp.Fprintf(tw, "End time\t%s\n", cfg.EndTime())
	tw.Flush()

	rand.Seed(123)

	if cmd.PrintOnly {
		return nil, nil, nil
	}

	db = NewDatabase(cfg)
	if err = db.Create(); err != nil {
		return nil, nil, err
	}

	groups := db.Info.RetentionPolicy(db.Info.DefaultRetentionPolicy).ShardGroups
	gens = make([][]ingen.SeriesGenerator, len(groups))
	for i := range gens {

		name := []byte("m0")

		sgi := &groups[i]
		gens[i] = make([]ingen.SeriesGenerator, cmd.Fields)
		for f := 0; f < cmd.Fields; f++ {
			tv := make([]gen.Sequence, len(tags))
			setTagVals(tags, tv)
			keys := make([]string, len(tags))
			setTagKeys("tag", keys)
			//vg := gen.NewIntegerRandomValuesSequence(cmd.PointsPerSeriesPerShard, sgi.StartTime, cfg.ShardDuration.Duration/time.Duration(cmd.PointsPerSeriesPerShard), 2)
			vg := gen.NewFloatRandomValuesSequence(cmd.PointsPerSeriesPerShard, sgi.StartTime, cfg.ShardDuration.Duration/time.Duration(cmd.PointsPerSeriesPerShard), float64(10*(f+1)))

			gens[i][f] = gen.NewSeriesGenerator(name, fmt.Sprintf("v%d", f), vg, gen.NewTagsValuesSequenceKeysValues(keys, tv))
		}
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
