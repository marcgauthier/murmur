package murmurgorm

import (
	"errors"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

type gormUser struct {
	ID        uint `gorm:"primaryKey"`
	Name      string
	Age       int
	Active    bool
	CreatedAt time.Time
	UpdatedAt time.Time
	Pets      []gormPet `gorm:"foreignKey:OwnerID"`
}

type gormPet struct {
	ID      uint `gorm:"primaryKey"`
	Name    string
	OwnerID uint
	Owner   gormUser `gorm:"foreignKey:OwnerID"`
}

// gormUserV2 evolves the users table: same table, one added column.
type gormUserV2 struct {
	ID   uint `gorm:"primaryKey"`
	Name string
	Nick string
}

func (gormUserV2) TableName() string { return "gorm_users" }

// openGORM bridges a fresh native database to *gorm.DB.
func openGORM(t *testing.T, cfg *gorm.Config) *gorm.DB {
	t.Helper()
	native := openNative(t)
	if cfg == nil {
		cfg = &gorm.Config{}
	}
	db, err := gorm.Open(Open(native), cfg)
	if err != nil {
		t.Fatal(err)
	}
	pool, ok := SQLDB(db)
	if !ok {
		t.Fatal("SQLDB accessor failed")
	}
	t.Cleanup(func() {
		if err := pool.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func TestGormOpen(t *testing.T) {
	db := openGORM(t, nil)
	if db.Dialector.Name() != "murmur" {
		t.Fatalf("dialect = %q", db.Dialector.Name())
	}
	native, ok := Native(db)
	if !ok || native == nil {
		t.Fatal("Native accessor failed")
	}
	if status, err := native.Status(); err != nil || status.Closed {
		t.Fatalf("native status: %v %+v", err, status)
	}
	if db.Migrator().HasTable(&gormUser{}) {
		t.Fatal("table exists before migrate")
	}
}

func TestGormAutoMigrateCRUD(t *testing.T) {
	db := openGORM(t, nil)
	if err := db.AutoMigrate(&gormUser{}); err != nil {
		t.Fatal(err)
	}
	if !db.Migrator().HasTable(&gormUser{}) {
		t.Fatal("table missing after migrate")
	}
	if !db.Migrator().HasColumn(&gormUser{}, "Name") {
		t.Fatal("column missing after migrate")
	}

	u := gormUser{Name: "ada", Age: 36, Active: true}
	if err := db.Create(&u).Error; err != nil {
		t.Fatal(err)
	}
	if u.ID == 0 {
		t.Fatal("ID not filled")
	}
	if u.CreatedAt.IsZero() || u.UpdatedAt.IsZero() {
		t.Fatalf("timestamps not managed: %+v", u)
	}

	var got gormUser
	if err := db.First(&got, u.ID).Error; err != nil {
		t.Fatal(err)
	}
	if got.Name != "ada" || got.Age != 36 || !got.Active {
		t.Fatalf("%+v", got)
	}
	if !got.CreatedAt.Equal(u.CreatedAt) {
		t.Fatalf("created_at round-trip: %v vs %v", got.CreatedAt, u.CreatedAt)
	}

	if err := db.Model(&gormUser{}).Where("id = ?", u.ID).Update("Age", 37).Error; err != nil {
		t.Fatal(err)
	}
	var age int
	if err := db.Model(&gormUser{}).Where("id = ?", u.ID).Pluck("Age", &age).Error; err != nil {
		t.Fatal(err)
	}
	if age != 37 {
		t.Fatal(age)
	}

	var count int64
	if err := db.Model(&gormUser{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	if err := db.Delete(&gormUser{}, u.ID).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.First(&gormUser{}, u.ID).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expected ErrRecordNotFound, got %v", err)
	}
}

func TestGormAssociations(t *testing.T) {
	db := openGORM(t, nil)
	if err := db.AutoMigrate(&gormUser{}, &gormPet{}); err != nil {
		t.Fatal(err)
	}
	u := gormUser{Name: "grace", Pets: []gormPet{{Name: "rex"}, {Name: "tom"}}}
	if err := db.Create(&u).Error; err != nil {
		t.Fatal(err)
	}
	var got gormUser
	if err := db.Preload("Pets").First(&got, u.ID).Error; err != nil {
		t.Fatal(err)
	}
	if len(got.Pets) != 2 {
		t.Fatalf("pets: %+v", got.Pets)
	}
	var pet gormPet
	if err := db.Preload("Owner").First(&pet, got.Pets[0].ID).Error; err != nil {
		t.Fatal(err)
	}
	if pet.Owner.Name != "grace" {
		t.Fatalf("owner: %+v", pet.Owner)
	}
}

func TestGormTransaction(t *testing.T) {
	db := openGORM(t, nil)
	if err := db.AutoMigrate(&gormUser{}); err != nil {
		t.Fatal(err)
	}
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&gormUser{Name: "commit"}).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	boom := errors.New("boom")
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&gormUser{Name: "rollback"}).Error; err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&gormUser{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	// Nested transactions need savepoints, which Murmur denies.
	err = db.Transaction(func(tx *gorm.DB) error {
		return tx.Transaction(func(tx2 *gorm.DB) error { return nil })
	})
	if !errors.Is(err, ErrSavepointsUnsupported) {
		t.Fatalf("nested tx: got %v", err)
	}
}

func TestGormDuplicateKey(t *testing.T) {
	// TranslateError opts into Translate, like stock drivers.
	db := openGORM(t, &gorm.Config{TranslateError: true})
	if err := db.AutoMigrate(&gormUser{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&gormUser{ID: 9, Name: "one"}).Error; err != nil {
		t.Fatal(err)
	}
	err := db.Create(&gormUser{ID: 9, Name: "two"}).Error
	if !errors.Is(err, gorm.ErrDuplicatedKey) {
		t.Fatalf("expected ErrDuplicatedKey, got %v", err)
	}
}

func TestGormRaw(t *testing.T) {
	db := openGORM(t, nil)
	if err := db.AutoMigrate(&gormUser{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("INSERT INTO `gorm_users`(`name`,`age`,`active`) VALUES(?,?,?)", "raw", 1, true).Error; err != nil {
		t.Fatal(err)
	}
	var names []string
	if err := db.Raw("SELECT `name` FROM `gorm_users` ORDER BY `id`").Scan(&names).Error; err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "raw" {
		t.Fatal(names)
	}
	type row struct {
		Name string
		Age  int
	}
	var r row
	if err := db.Raw("SELECT `name`,`age` FROM `gorm_users` LIMIT 1").Scan(&r).Error; err != nil {
		t.Fatal(err)
	}
	if r.Name != "raw" || r.Age != 1 {
		t.Fatalf("%+v", r)
	}
}

func TestGormMigratorOps(t *testing.T) {
	db := openGORM(t, nil)
	if err := db.AutoMigrate(&gormUser{}); err != nil {
		t.Fatal(err)
	}
	m := db.Migrator()
	types, err := m.ColumnTypes(&gormUser{})
	if err != nil {
		t.Fatal(err)
	}
	if len(types) == 0 {
		t.Fatal("no column types")
	}
	if err := db.AutoMigrate(&gormUserV2{}); err != nil {
		t.Fatal(err)
	}
	if !m.HasColumn(&gormUserV2{}, "Nick") {
		t.Fatal("added column missing")
	}
	if err := m.DropColumn(&gormUserV2{}, "Nick"); err != nil {
		t.Fatal(err)
	}
	if m.HasColumn(&gormUserV2{}, "Nick") {
		t.Fatal("dropped column present")
	}
	tables, err := m.GetTables()
	if err != nil || len(tables) == 0 {
		t.Fatalf("tables=%v err=%v", tables, err)
	}
	if err := m.DropTable(&gormUser{}); err != nil {
		t.Fatal(err)
	}
	if m.HasTable(&gormUser{}) {
		t.Fatal("table present after drop")
	}
}

func TestGormSessionModes(t *testing.T) {
	db := openGORM(t, nil)
	if err := db.AutoMigrate(&gormUser{}); err != nil {
		t.Fatal(err)
	}
	prep := db.Session(&gorm.Session{PrepareStmt: true})
	if err := prep.Create(&gormUser{Name: "prep"}).Error; err != nil {
		t.Fatal(err)
	}
	var u gormUser
	if err := prep.Where("name = ?", "prep").First(&u).Error; err != nil {
		t.Fatal(err)
	}
	plain := db.Session(&gorm.Session{SkipDefaultTransaction: true})
	if err := plain.Create(&gormUser{Name: "plain"}).Error; err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&gormUser{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestGormConcurrent(t *testing.T) {
	db := openGORM(t, nil)
	if err := db.AutoMigrate(&gormUser{}); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if err := db.Create(&gormUser{Name: "c", Age: i}).Error; err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	var count int64
	if err := db.Model(&gormUser{}).Count(&count).Error; err != nil || count != 8 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestNativeAdminAlongside(t *testing.T) {
	db := openGORM(t, nil)
	if err := db.AutoMigrate(&gormUser{}); err != nil {
		t.Fatal(err)
	}
	native, ok := Native(db)
	if !ok {
		t.Fatal("no native handle")
	}
	if err := db.Create(&gormUser{Name: "admin"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := native.Flush(); err != nil {
		t.Fatal(err)
	}
	schema, err := native.Schema()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, tbl := range schema.Tables {
		if tbl.Name == "gorm_users" {
			found = true
		}
	}
	if !found {
		t.Fatalf("gorm_users missing from native schema: %+v", schema.Tables)
	}
}
