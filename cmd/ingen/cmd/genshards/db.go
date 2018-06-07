package genshards

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/services/meta"
	"github.com/influxdata/ingen"
)

type DBConfig struct {
	DataPath      string `toml:"data-path"`
	MetaPath      string `toml:"meta-path"`
	Database      string
	RP            string
	StartTime     time.Time `toml:"start-time"`
	ShardCount    int       `toml:"shard-count"`
	ShardDuration duration  `toml:"shard-duration"`
}

func (cfg *DBConfig) Validate() error {
	// build default values
	def := &configDefaults{}
	WalkConfig(def, cfg)

	// validate
	val := &configValidator{}
	WalkConfig(val, cfg)
	return val.Err()
}

// TimeSpan returns the total duration for which the data set.
func (cfg *DBConfig) TimeSpan() time.Duration {
	return cfg.ShardDuration.Duration * time.Duration(cfg.ShardCount)
}

func (cfg *DBConfig) EndTime() time.Time {
	return cfg.StartTime.Add(cfg.TimeSpan())
}

type Database struct {
	Info      *meta.DatabaseInfo
	ShardPath string

	dataPath      string
	metaPath      string
	database      string
	rp            string
	startTime     time.Time
	shardCount    int
	shardDuration time.Duration
}

func NewDatabase(cfg *DBConfig) *Database {
	return &Database{
		dataPath:      cfg.DataPath,
		metaPath:      cfg.MetaPath,
		database:      cfg.Database,
		rp:            cfg.RP,
		startTime:     cfg.StartTime,
		shardCount:    cfg.ShardCount,
		shardDuration: cfg.ShardDuration.Duration,
	}
}

func (db *Database) Create() (err error) {
	client := meta.NewClient(&meta.Config{Dir: db.metaPath})
	if err = client.Open(); err != nil {
		return err
	}
	defer client.Close()

	// drop and recreate database
	client.DropDatabase(db.database)
	dbpath := filepath.Join(db.dataPath, db.database)
	if err = os.RemoveAll(dbpath); err != nil {
		return err
	}

	var rp meta.RetentionPolicySpec
	rp.ShardGroupDuration = db.shardDuration
	rp.Name = db.rp
	db.Info, err = client.CreateDatabaseWithRetentionPolicy(db.database, &rp)
	if err != nil {
		return err
	}

	db.ShardPath = filepath.Join(dbpath, db.Info.DefaultRetentionPolicy)
	return db.createShardGroups(client)
}

func (db *Database) createShardGroups(client *meta.Client) error {
	ts := db.startTime.Truncate(db.shardDuration).UTC()

	var err error
	groups := make([]*meta.ShardGroupInfo, db.shardCount)
	for i := 0; i < db.shardCount; i++ {
		groups[i], err = client.CreateShardGroup(db.database, db.Info.DefaultRetentionPolicy, ts)
		if err != nil {
			return err
		}
		if err = os.MkdirAll(filepath.Join(db.ShardPath, strconv.Itoa(int(groups[i].ID))), 0777); err != nil {
			return err
		}
		ts = ts.Add(db.shardDuration)
	}

	db.Info = client.Database(db.database)

	return nil
}

type Visitor interface {
	Visit(node Node) Visitor
}

type Node interface{ node() }

func (*DBConfig) node() {}

func WalkConfig(v Visitor, node Node) {
	if v = v.Visit(node); v == nil {
		return
	}

	switch n := node.(type) {
	case *DBConfig:

	default:
		panic(fmt.Sprintf("WalkConfig: unexpected node type %T", n))
	}
}

type configValidator struct {
	errs []error
}

func (v *configValidator) Visit(node Node) Visitor {
	switch n := node.(type) {
	case *DBConfig:
		if n.StartTime.Add(n.TimeSpan()).After(time.Now()) {
			v.errs = append(v.errs, fmt.Errorf("start time must be ≤ %s", time.Now().Truncate(n.ShardDuration.Duration).UTC().Add(-n.TimeSpan())))
		}
	}

	return v
}
func (v *configValidator) Err() error {
	return ingen.NewErrorList(v.errs)
}

type configDefaults struct {
	tagN int
}

func (v *configDefaults) Visit(node Node) Visitor {
	switch n := node.(type) {
	case *DBConfig:
		if n.DataPath == "" {
			n.DataPath = "${HOME}/.influxdb/data"
		}
		if n.MetaPath == "" {
			n.MetaPath = "${HOME}/.influxdb/meta"
		}
		if n.Database == "" {
			n.Database = "db"
		}
		if n.RP == "" {
			n.RP = "autogen"
		}
		if n.ShardDuration.Duration == 0 {
			n.ShardDuration.Duration = 24 * time.Hour
		}
		if n.ShardCount == 0 {
			n.ShardCount = 1
		}
		if n.StartTime.IsZero() {
			n.StartTime = time.Now().Truncate(n.ShardDuration.Duration).Add(-n.TimeSpan())
		}
	}

	return v
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}
