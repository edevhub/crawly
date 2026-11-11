package pgstore

import (
	"context"
	"database/sql"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib" // the database/sql driver goose runs on
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"
)

// DefaultMigrationsTable is the table that records which migrations were applied.
const DefaultMigrationsTable = "crawly_migrations"

//go:embed migrations/*.sql
var migrationFiles embed.FS

// migrations is the directory of the migration files.
func migrations() fs.FS {
	dir, err := fs.Sub(migrationFiles, "migrations")
	if err != nil {
		panic(err) // the directory is part of the build
	}
	return dir
}

// SchemaVersion is the version of the newest migration in this build.
func SchemaVersion() int64 {
	names, err := fs.Glob(migrations(), "*.sql")
	if err != nil {
		panic(err) // the pattern is constant
	}
	var latest int64
	for _, name := range names {
		if v, err := goose.NumericComponent(name); err == nil && v > latest {
			latest = v
		}
	}
	return latest
}

// Migration is one migration and whether the database has it.
type Migration struct {
	Version   int64
	Name      string    // the file name
	AppliedAt time.Time // zero: pending
}

// Migrator applies the migrations of this build to a database. The connection must
// reach the database directly: the lock that keeps two migrators apart belongs to a
// session, which a transaction pooler does not keep.
type Migrator struct {
	db *sql.DB
	p  *goose.Provider
}

// NewMigrator connects to the database. table is where the applied migrations are
// recorded; empty means DefaultMigrationsTable. It may name a schema: "ops.crawly".
func NewMigrator(dsn, table string, log *slog.Logger) (*Migrator, error) {
	if table == "" {
		table = DefaultMigrationsTable
	}
	if _, err := tableIdent(table); err != nil {
		return nil, err
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("pgstore: %w", err)
	}
	locker, err := lock.NewPostgresSessionLocker()
	if err == nil {
		opts := []goose.ProviderOption{goose.WithTableName(table), goose.WithSessionLocker(locker)}
		if log != nil {
			opts = append(opts, goose.WithSlog(log))
		}
		var p *goose.Provider
		if p, err = goose.NewProvider(goose.DialectPostgres, db, migrations(), opts...); err == nil {
			return &Migrator{db: db, p: p}, nil
		}
	}
	_ = db.Close()
	return nil, fmt.Errorf("pgstore: %w", err)
}

// Close releases the connection.
func (m *Migrator) Close() error { return m.db.Close() }

// Up applies every pending migration and returns the ones it applied, in order.
func (m *Migrator) Up(ctx context.Context) ([]Migration, error) {
	results, err := m.p.Up(ctx)
	applied := make([]Migration, 0, len(results))
	for _, r := range results {
		if r.Error == nil && r.Source != nil {
			applied = append(applied, Migration{Version: r.Source.Version, Name: baseName(r.Source.Path)})
		}
	}
	if err != nil {
		return applied, fmt.Errorf("pgstore: migrating: %w", err)
	}
	return applied, nil
}

// Status lists every migration of this build, in order.
func (m *Migrator) Status(ctx context.Context) ([]Migration, error) {
	status, err := m.p.Status(ctx)
	if err != nil {
		return nil, fmt.Errorf("pgstore: reading the migrations: %w", err)
	}
	out := make([]Migration, 0, len(status))
	for _, st := range status {
		mig := Migration{Version: st.Source.Version, Name: baseName(st.Source.Path)}
		if st.State == goose.StateApplied {
			mig.AppliedAt = st.AppliedAt
		}
		out = append(out, mig)
	}
	return out, nil
}

func baseName(path string) string {
	if i := strings.LastIndexByte(path, '/'); i >= 0 {
		return path[i+1:]
	}
	return path
}

// tableIdent is the migrations table as a quoted identifier.
func tableIdent(table string) (string, error) {
	parts := strings.Split(table, ".")
	if len(parts) > 2 || slices.Contains(parts, "") {
		return "", fmt.Errorf("pgstore: migrations table %q: want a table name, or schema.table", table)
	}
	return pgx.Identifier(parts).Sanitize(), nil
}

// ErrNotMigrated reports a database whose schema is older than this build needs.
var ErrNotMigrated = errors.New("pgstore: the database schema is not up to date; run the migrations first")

const codeUndefinedTable = "42P01"

// checkSchema makes sure the database has every migration of this build. The store
// does not migrate on its own: a schema change is a step of the rollout.
func (s *Store) checkSchema(ctx context.Context) error {
	table, err := tableIdent(s.migrationsTable)
	if err != nil {
		return err
	}
	var have int64
	err = s.db.QueryRow(ctx, "SELECT coalesce(max(version_id), 0) FROM "+table+" WHERE is_applied").Scan(&have)
	switch need := SchemaVersion(); {
	case pgCode(err) == codeUndefinedTable:
		return fmt.Errorf("%w (no table %s)", ErrNotMigrated, s.migrationsTable)
	case err != nil:
		return wrap(err)
	case have < need:
		return fmt.Errorf("%w (version %d, this build needs %d)", ErrNotMigrated, have, need)
	case have > need:
		// A newer build migrated the database already, as in a rolling update.
		s.log.Warn("pgstore: the database schema is newer than this build", "database", have, "build", need)
	}
	return nil
}
