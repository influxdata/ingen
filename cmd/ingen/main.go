package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/ingen"
)

// Main represents the main program execution.
type Main struct {
	Stdin  io.Reader
	Stdout io.Writer
	Stderr io.Writer

	gen *ingen.Generator
}

func main() {
	m := NewMain()
	if err := m.ParseFlags(os.Args[1:]...); err != nil {
		os.Exit(1)
	}
	if err := m.gen.Run(context.Background()); err != nil {
		fmt.Fprintln(m.Stderr, err)
		os.Exit(1)
	}
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
		gen:    ingen.NewGenerator(),
	}
}

// ParseFlags parses the command line flags from args and returns an options set.
func (m *Main) ParseFlags(args ...string) error {
	m.gen.Stdout = m.Stdout
	m.gen.Stderr = m.Stderr

	fs := flag.NewFlagSet("", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	fs.BoolVar(&m.gen.PrintOnly, "print", false, "Print data spec only")
	fs.StringVar(&m.gen.DataPath, "data-path", "", "path to InfluxDB data")
	fs.StringVar(&m.gen.MetaPath, "meta-path", "", "path to InfluxDB meta")
	fs.IntVar(&m.gen.Concurrency, "c", 1, "Concurrency")
	fs.IntVar(&m.gen.PointsPerSeriesPerShard, "p", 100, "Points per series per shard")
	fs.IntVar(&m.gen.ShardCount, "shards", 1, "Number of shards to create")
	tags := fs.String("t", "10,10,10", "Tag cardinality")
	fs.StringVar(&m.gen.Database, "db", "db", "Database to create")
	fs.DurationVar(&m.gen.ShardDuration, "shard-duration", 24*time.Hour, "Shard duration (default 24h)")
	fs.StringVar(&m.gen.StartTime, "start-time", "", "Start time (default now - (shards * shard-duration))")

	if err := fs.Parse(args); err != nil {
		return err
	}

	// Parse tag cardinalities.
	m.gen.Tags = []int{}
	for _, s := range strings.Split(*tags, ",") {
		v, err := strconv.Atoi(s)
		if err != nil {
			return fmt.Errorf("cannot parse tag cardinality: %s", s)
		}
		m.gen.Tags = append(m.gen.Tags, v)
	}

	return nil
}
