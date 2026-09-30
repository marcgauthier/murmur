package gormmigrate_test

import (
	"testing"
	"time"

	murmur "github.com/marcgauthier/murmur/gormmurmur"
	"github.com/marcgauthier/murmur/tests-live/gormharness"
)

type GadgetV1 struct {
	murmur.Model
	Name string
}

func (GadgetV1) TableName() string { return "gadgets" }

type GadgetV2 struct {
	murmur.Model
	Name  string
	Color *string
}

func (GadgetV2) TableName() string { return "gadgets" }

// TestGormMigrateEvolvesLiveCluster proves schema evolution through
// GORM against a replicating cluster: AutoMigrate on one node
// publishes a new epoch, the peer adopts it, both nodes read and
// write the new column, and a full rolling restart preserves data
// with AutoMigrate staying idempotent.
func TestGormMigrateEvolvesLiveCluster(t *testing.T) {
	cluster := gormharness.NewCluster(t, "gorm-migrate", 2, &GadgetV1{})

	g0 := cluster.Nodes[0].GDB
	if err := g0.Create(&GadgetV1{Name: "v1-row"}).Error; err != nil {
		t.Fatalf("create v1: %v", err)
	}
	cluster.WaitForCount(1, &GadgetV1{}, 1, 30*time.Second)

	// Additive migration on node 0 only; node 1 must adopt it.
	if err := g0.AutoMigrate(&GadgetV2{}); err != nil {
		t.Fatalf("automigrate v2: %v", err)
	}
	epoch0 := cluster.Nodes[0].DB.Status().SchemaEpoch
	if epoch0 < 2 {
		t.Fatalf("node 0 epoch = %d after migration, want >= 2", epoch0)
	}
	cluster.WaitSchemaEpoch(30 * time.Second)

	// The adopter writes the new column; the originator reads it back.
	teal := "teal"
	if err := cluster.Nodes[1].GDB.Create(&GadgetV2{Name: "v2-row", Color: &teal}).Error; err != nil {
		t.Fatalf("create v2 on adopter: %v", err)
	}
	cluster.WaitForCount(0, &GadgetV2{}, 2, 30*time.Second)
	var back GadgetV2
	if err := g0.First(&back, "name = ?", "v2-row").Error; err != nil {
		t.Fatalf("read v2 row on originator: %v", err)
	}
	if back.Color == nil || *back.Color != "teal" {
		t.Fatalf("migrated column = %+v, want teal", back.Color)
	}
	var legacy GadgetV2
	if err := g0.First(&legacy, "name = ?", "v1-row").Error; err != nil {
		t.Fatalf("read legacy row: %v", err)
	}
	if legacy.Color != nil {
		t.Fatalf("legacy row color = %q, want NULL", *legacy.Color)
	}

	// Rolling restart of the whole cluster from live exports.
	cluster.RestartNode(0)
	cluster.WaitConnected(30 * time.Second)
	cluster.RestartNode(1)
	cluster.WaitConnected(30 * time.Second)

	cluster.WaitForCount(0, &GadgetV2{}, 2, 30*time.Second)
	cluster.WaitForCount(1, &GadgetV2{}, 2, 30*time.Second)
	var after GadgetV2
	if err := cluster.Nodes[1].GDB.First(&after, "name = ?", "v2-row").Error; err != nil {
		t.Fatalf("read after restart: %v", err)
	}
	if after.Color == nil || *after.Color != "teal" {
		t.Fatalf("column after restart = %+v, want teal", after.Color)
	}

	// Migration stays idempotent: no new epoch.
	before := cluster.Nodes[0].DB.Status().SchemaEpoch
	if err := cluster.Nodes[0].GDB.AutoMigrate(&GadgetV2{}); err != nil {
		t.Fatalf("repeat automigrate: %v", err)
	}
	if after := cluster.Nodes[0].DB.Status().SchemaEpoch; after != before {
		t.Fatalf("epoch advanced %d -> %d on idempotent migrate", before, after)
	}
}
