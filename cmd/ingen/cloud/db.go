package cloud

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
	DataPath   string `toml:"data-path"`
	ShardCount int    `toml:"shard-count"`
	NodeCount  int    `toml:"node-count"`
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

type Database struct {
	Info      *meta.DatabaseInfo
	ShardPath string

	dataPath   string
	database   string
	rp         string
	startTime  time.Time
	shardCount int
}

func NewDatabase(cfg *DBConfig) *Database {
	return &Database{
		dataPath:   cfg.DataPath,
		database:   "db",
		rp:         "rp",
		startTime:  time.Now(),
		shardCount: cfg.ShardCount,
	}
}

func (db *Database) Create() (err error) {
	dbpath := filepath.Join(db.dataPath, db.database)
	if err = os.RemoveAll(dbpath); err != nil {
		return err
	}

	client := newMetaClient([]int{0})
	db.Info = client.db

	db.ShardPath = filepath.Join(dbpath, db.Info.DefaultRetentionPolicy)
	return db.createShardGroups()
}

func (db *Database) createShardGroups() error {
	var err error
	groups := db.Info.RetentionPolicies[0].ShardGroups
	for i := 0; i < db.shardCount; i++ {
		if err = os.MkdirAll(filepath.Join(db.ShardPath, strconv.Itoa(int(groups[i].ID))), 0777); err != nil {
			return err
		}
	}

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
		WalkConfig(v, n)

	default:
		panic(fmt.Sprintf("WalkConfig: unexpected node type %T", n))
	}
}

type configValidator struct {
	errs []error
}

func (v *configValidator) Visit(node Node) Visitor {
	switch node.(type) {
	case *DBConfig:
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
			n.DataPath = "out/data"
		}
		if n.ShardCount == 0 {
			n.ShardCount = 1
		}
		if n.NodeCount == 0 {
			n.NodeCount = 1
		}
	}

	return v
}
