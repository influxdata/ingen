package ingen

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/influxdb/tsdb"
	"github.com/influxdata/influxdb/tsdb/engine/tsm1"
)

// ErrorList is a simple error aggregator to return multiple errors as one.
type ErrorList []error

func (el ErrorList) Error() string {
	var msg string
	for _, err := range el {
		msg = fmt.Sprintf("%s%s\n", msg, err)
	}
	return msg
}

// Generator represents the main program execution.
type Generator struct {
	mu        sync.Mutex
	writtenN  int
	startTime time.Time
	now       time.Time

	meta      *meta.Client
	db        *meta.DatabaseInfo
	shardPath string

	Stdout io.Writer
	Stderr io.Writer

	Verbose   bool
	PrintOnly bool

	DataPath                string
	MetaPath                string
	ShardCount              int
	Concurrency             int
	Tags                    []int // tag cardinalities
	TagFormats              []string
	PointsPerSeriesPerShard int
	FieldsPerPoint          int

	Database      string
	ShardDuration time.Duration // Set a custom shard duration.
	StartTime     string        // Set a custom start time.
}

// NewGenerator returns a new instance of Generator.
func NewGenerator() *Generator {
	// Create an Simulator object with reasonable defaults.
	return &Generator{
		Concurrency: 1,
		Tags:        []int{10, 10, 10},
		PointsPerSeriesPerShard: 100,
		FieldsPerPoint:          1,
		Database:                "db",
		ShardDuration:           24 * time.Hour,
		ShardCount:              1,
	}
}

// Validate parses the command line flags.
func (s *Generator) Validate() error {
	var el ErrorList

	if s.DataPath == "" {
		el = append(el, errors.New("missing data-path"))
	}

	if s.MetaPath == "" {
		el = append(el, errors.New("missing meta-path"))
	}

	if s.FieldsPerPoint < 1 {
		el = append(el, errors.New("number of fields must be > 0"))
	}

	if s.StartTime != "" {
		if t, err := time.Parse(time.RFC3339, s.StartTime); err != nil {
			el = append(el, err)
		} else {
			s.startTime = t.UTC()
		}
	} else {
		s.startTime = time.Now().Truncate(s.ShardDuration).Add(-s.TimeSpan())
	}

	if s.startTime.Add(s.TimeSpan()).After(time.Now()) {
		el = append(el, fmt.Errorf("start time must be ≤ %s", time.Now().Truncate(s.ShardDuration).UTC().Add(-s.TimeSpan())))
	}

	if len(el) > 0 {
		return el
	}

	return nil
}

func (s *Generator) Run(ctx context.Context) error {
	// check valid settings before starting
	err := s.Validate()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Print settings.
	fmt.Fprintf(s.Stdout, "Data Path: %s\n", s.DataPath)
	fmt.Fprintf(s.Stdout, "Meta Path: %s\n", s.MetaPath)
	fmt.Fprintf(s.Stdout, "Concurrency: %d\n", s.Concurrency)
	fmt.Fprintf(s.Stdout, "Tag cardinalities: %+v\n", s.Tags)
	fmt.Fprintf(s.Stdout, "Points per series per shard: %d\n", s.PointsPerSeriesPerShard)
	fmt.Fprintf(s.Stdout, "Total points per shard: %d\n", s.PointsPerShardN())
	fmt.Fprintf(s.Stdout, "Total series: %d\n", s.SeriesN())
	fmt.Fprintf(s.Stdout, "Total points: %d\n", s.PointN())
	fmt.Fprintf(s.Stdout, "Total fields per point: %d\n", s.FieldsPerPoint)
	fmt.Fprintf(s.Stdout, "Shard Count: %d\n", s.ShardCount)
	fmt.Fprintf(s.Stdout, "Database: %s (Shard duration: %s)\n", s.Database, s.ShardDuration)
	fmt.Fprintf(s.Stdout, "Start time: %s\n", s.startTime)
	fmt.Fprintf(s.Stdout, "End time: %s\n", s.EndTime())
	fmt.Fprintln(s.Stdout)

	if s.PrintOnly {
		return nil
	}

	// Initialize database.
	if err := s.setup(); err != nil {
		return err
	}

	s.now = time.Now().UTC()

	s.createShardGroups()

	var wg sync.WaitGroup
	wg.Add(s.ShardCount)

	limit := make(chan struct{}, s.Concurrency)
	for i := 0; i < s.Concurrency; i++ {
		limit <- struct{}{}
	}

	for i := 0; i < s.ShardCount; i++ {
		go func(n int) {
			<-limit
			defer func() {
				wg.Done()
				limit <- struct{}{}
			}()

			if err = s.writeShard(n); err != nil {
				fmt.Fprintf(s.Stderr, "error writing shard %d: %s", s.db.RetentionPolicy("autogen").ShardGroups[n].ID, err.Error())
			}
		}(i)
	}

	wg.Wait()

	// Report stats.
	elapsed := time.Since(s.now)
	fmt.Fprintln(s.Stdout, "")
	fmt.Fprintf(s.Stdout, "Total time: %0.1f seconds\n", elapsed.Seconds())

	return nil
}

// WrittenN returns the total number of points written.
func (s *Generator) WrittenN() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writtenN
}

// TagsN returns the total number of tags.
func (s *Generator) TagsN() int {
	tagTotal := s.Tags[0]
	for _, v := range s.Tags[1:] {
		tagTotal *= v
	}
	return tagTotal
}

func (s *Generator) EndTime() time.Time {
	return s.startTime.Add(s.TimeSpan())
}

// TimeSpan returns the total duration for which the data set will be generated.
func (s *Generator) TimeSpan() time.Duration {
	return s.ShardDuration * time.Duration(s.ShardCount)
}

// SeriesN returns the total number of series to write.
func (s *Generator) SeriesN() int {
	return s.TagsN()
}

// PointN returns the total number of points to write.
func (s *Generator) PointN() int {
	return int(s.PointsPerSeriesPerShard) * s.SeriesN() * s.ShardCount
}

func (s *Generator) PointsPerShardN() int {
	return int(s.PointsPerSeriesPerShard) * s.SeriesN()
}

func (s *Generator) setup() error {
	var err error

	cfg := meta.NewConfig()
	cfg.Dir = s.MetaPath
	s.meta = meta.NewClient(cfg)
	if err = s.meta.Open(); err != nil {
		return err
	}

	// drop and recreate database
	s.meta.DropDatabase(s.Database)
	dbpath := filepath.Join(s.DataPath, s.Database)
	os.RemoveAll(dbpath)

	var rp meta.RetentionPolicySpec
	rp.ShardGroupDuration = s.ShardDuration
	rp.Name = "autogen"
	s.db, err = s.meta.CreateDatabaseWithRetentionPolicy(s.Database, &rp)
	if err != nil {
		return err
	}

	s.shardPath = filepath.Join(dbpath, s.db.DefaultRetentionPolicy)

	// determine tag widths
	tw := int(math.Ceil(math.Log10(float64(len(s.Tags)))))
	for _, c := range s.Tags {
		w := int(math.Ceil(math.Log10(float64(c))))
		s.TagFormats = append(s.TagFormats, fmt.Sprintf("tag%%0%dd=value%%0%dd", tw, w))

	}

	return nil
}

func (s *Generator) writeShard(n int) error {
	sgi := &s.db.RetentionPolicy("autogen").ShardGroups[n]
	counts := make([]int, len(s.Tags))
	start := sgi.StartTime
	delta := s.ShardDuration / time.Duration(s.PointsPerShardN())

	ww := newShardWriter(sgi, s.shardPath)
	defer ww.Close()

	vals := make(tsm1.Values, tsdb.DefaultMaxPointsPerBlock)

	for i := 0; i < s.SeriesN(); i++ {
		key := "m0"
		for j, count := range counts {
			key += ","
			key += fmt.Sprintf(s.TagFormats[j], j, count)
		}

		key = tsm1.SeriesFieldKey(key, "v0")
		keyb := []byte(key)

		// write write write
		tp := s.PointsPerSeriesPerShard
		for tp > 0 {
			pc := min(tp, tsdb.DefaultMaxPointsPerBlock)
			data := vals[:pc]
			for p := range data {
				data[p] = tsm1.NewIntegerValue(start.UnixNano(), 1)
				start = start.Add(delta)
			}
			ww.Write(keyb, data)
			tp -= pc
		}

		if err := ww.Err(); err != nil {
			return err
		}

		// Increment next tag value.
		for v := len(counts) - 1; v >= 0; v-- {
			counts[v]++
			if counts[v] < s.Tags[v] {
				break
			}
			counts[v] = 0 // reset to zero, increment next value
		}
	}

	return nil
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// nextTSMWriter returns the next TSMWriter for the Converter.
func (s *Generator) nextTSMWriter(sgi *meta.ShardGroupInfo, gen, seq int) (tsm1.TSMWriter, error) {
	fileName := filepath.Join(s.shardPath, strconv.Itoa(int(sgi.ID)), fmt.Sprintf("%09d-%09d.%s", gen, seq, tsm1.TSMFileExtension))

	fd, err := os.OpenFile(fileName, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	// Create the writer for the new TSM file.
	w, err := tsm1.NewTSMWriter(fd)
	if err != nil {
		return nil, err
	}

	return w, nil
}

func (s *Generator) close() {
	if s.meta != nil {
		s.meta.Close()
	}
}
func (s *Generator) createShardGroups() error {
	ts := s.startTime.Truncate(s.ShardDuration).UTC()

	var err error
	groups := make([]*meta.ShardGroupInfo, s.ShardCount)
	for i := 0; i < s.ShardCount; i++ {
		groups[i], err = s.meta.CreateShardGroup(s.Database, s.db.DefaultRetentionPolicy, ts)
		if err != nil {
			return err
		}
		if err = os.MkdirAll(filepath.Join(s.shardPath, strconv.Itoa(int(groups[i].ID))), 0777); err != nil {
			return err
		}
		ts = ts.Add(s.ShardDuration)
	}

	s.db = s.meta.Database(s.Database)

	for _, sgi := range s.db.RetentionPolicy(s.db.DefaultRetentionPolicy).ShardGroups {
		fmt.Fprintf(s.Stderr, "%4d  %v  %v\n", sgi.ID, sgi.StartTime, sgi.EndTime)
	}

	return nil
}
