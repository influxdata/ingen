package ingen

import (
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/services/meta"
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
