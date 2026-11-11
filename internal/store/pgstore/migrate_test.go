package pgstore_test

import (
	"errors"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/edevhub/crawly/internal/store"
	"github.com/edevhub/crawly/internal/store/pgstore"
	"github.com/edevhub/crawly/internal/store/pgstore/pgtest"
)

func tableExists(t *testing.T, dsn, table string) bool {
	t.Helper()
	conn := must(pgx.Connect(ctx, dsn))(t)
	defer func() { _ = conn.Close(ctx) }()
	var there bool
	if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&there); err != nil {
		t.Fatal(err)
	}
	return there
}

// The store does not create its schema: it refuses a database without the migrations.
func TestStoreNeedsMigrations(t *testing.T) {
	dsn := pgtest.Empty(t)
	if _, err := pgstore.New(ctx, dsn, limits(store.Limits{})); !errors.Is(err, pgstore.ErrNotMigrated) {
		t.Fatalf("a store on an empty database: %v", err)
	}
	pgtest.Migrate(t, dsn, "")
	s := open(t, dsn, limits(store.Limits{}))
	createRun(t, s, "r")
}

// The migrations are recorded in the table the user names, and the store reads the
// version from the same table.
func TestMigrationsTable(t *testing.T) {
	dsn := pgtest.Empty(t)
	const table = "my_schema_history"
	applied := pgtest.Migrate(t, dsn, table)
	if len(applied) == 0 || applied[len(applied)-1].Version != pgstore.SchemaVersion() {
		t.Fatalf("applied %+v, the build is at version %d", applied, pgstore.SchemaVersion())
	}
	if !tableExists(t, dsn, table) || tableExists(t, dsn, pgstore.DefaultMigrationsTable) {
		t.Fatal("the migrations were not recorded in the named table only")
	}
	if again := pgtest.Migrate(t, dsn, table); len(again) != 0 {
		t.Fatalf("a second run applied %+v", again)
	}

	if _, err := pgstore.New(ctx, dsn, limits(store.Limits{})); !errors.Is(err, pgstore.ErrNotMigrated) {
		t.Fatalf("a store that reads the default table: %v", err)
	}
	s := open(t, dsn, limits(store.Limits{}), pgstore.WithMigrationsTable(table))
	createRun(t, s, "r")

	m := must(pgstore.NewMigrator(dsn, table, nil))(t)
	defer func() { _ = m.Close() }()
	status := must(m.Status(ctx))(t)
	if len(status) != len(applied) {
		t.Fatalf("status lists %d migrations, %d were applied", len(status), len(applied))
	}
	for _, mig := range status {
		if mig.AppliedAt.IsZero() {
			t.Errorf("%s is pending after the migration", mig.Name)
		}
	}
}

func TestMigrationsTableName(t *testing.T) {
	for _, bad := range []string{"a.b.c", ".x", "x."} {
		if _, err := pgstore.NewMigrator("postgres://localhost/x", bad, nil); err == nil {
			t.Errorf("%q was accepted as a table name", bad)
		}
	}
}

// Replicas of a rollout may all run the migrations: one applies them, the others wait
// and find nothing to do.
func TestConcurrentMigrations(t *testing.T) {
	dsn := pgtest.Empty(t)
	const n = 4
	counts := make([]int, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			m, err := pgstore.NewMigrator(dsn, "", nil)
			if err != nil {
				errs[i] = err
				return
			}
			defer func() { _ = m.Close() }()
			applied, err := m.Up(ctx)
			counts[i], errs[i] = len(applied), err
		})
	}
	wg.Wait()
	total := 0
	for i := range n {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		total += counts[i]
	}
	if total == 0 || total > int(pgstore.SchemaVersion()) {
		t.Fatalf("the migrators applied %v migrations between them", counts)
	}
	open(t, dsn, limits(store.Limits{}))
}
