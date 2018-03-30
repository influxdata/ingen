package cloud

import (
	"math"
	"sort"
	"time"

	"github.com/influxdata/influxdb/services/meta"
)

func newMetaClient(shards []int) *storageMeta {
	shardInfos := make([]meta.ShardInfo, 0, len(shards))
	for _, id := range shards {
		shardInfos = append(shardInfos, meta.ShardInfo{
			ID: uint64(id),
		})
	}
	sort.Slice(shardInfos, func(i, j int) bool {
		return shardInfos[i].ID < shardInfos[j].ID
	})

	return &storageMeta{db: &meta.DatabaseInfo{
		Name: "db",
		DefaultRetentionPolicy: "rp",
		RetentionPolicies: []meta.RetentionPolicyInfo{
			{
				Name:               "rp",
				ReplicaN:           1,
				Duration:           time.Duration(0),
				ShardGroupDuration: time.Duration(0),
				ShardGroups: []meta.ShardGroupInfo{
					{
						ID:        1,
						StartTime: time.Unix(0, 0),
						EndTime:   time.Unix(math.MaxInt64, math.MaxInt64),
						Shards:    shardInfos,
					},
				},
			},
		},
	}}
}

type storageMeta struct {
	db *meta.DatabaseInfo
}

func (s *storageMeta) Open() error                             { return nil }
func (s *storageMeta) Close() error                            { return nil }
func (s *storageMeta) Database(name string) *meta.DatabaseInfo { return s.db }

func (s *storageMeta) ShardGroupsByTimeRange(database, policy string, min, max time.Time) (a []meta.ShardGroupInfo, err error) {
	return s.db.RetentionPolicies[0].ShardGroups, nil
}
