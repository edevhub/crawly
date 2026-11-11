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
	// The indexes that migration 2 declares on tasks are built here, on the partitions
	// that exist, without a lock that stops writes. Every call checks them, so a build
	// that was interrupted is finished by the next one.
	if err := buildTaskIndexes(ctx, m.db); err != nil {
		return applied, fmt.Errorf("pgstore: building the indexes of tasks: %w", err)
	}
	return applied, nil
}

// partitionsOfTasks lists the partitions of tasks.
func partitionsOfTasks(ctx context.Context, conn *sql.Conn) ([]string, error) {
	rows, err := conn.QueryContext(ctx, `SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = to_regclass('tasks') ORDER BY c.relname`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var parts []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		parts = append(parts, name)
	}
	return parts, rows.Err()
}

// taskIndexes are the indexes migration 2 declares ON ONLY tasks: the partitions get
// theirs from buildTaskIndexes, and the partitions created later from the parent.
var taskIndexes = []struct{ parent, suffix, columns string }{
	{"tasks_claim_h", "claim_h", `(run_no, queue_key, pool_id, handler, priority DESC, task_id)
		INCLUDE (not_before, expires_at) WHERE state = 'pending'`},
	{"tasks_next_h", "next_h", `(run_no, queue_key, pool_id, handler, not_before) WHERE state = 'pending'`},
}

// buildTaskIndexes gives every partition of tasks the indexes of taskIndexes. Each is
// created with CREATE INDEX CONCURRENTLY, which waits for the transactions that run
// but never stops a write, and attached to the parent index. It leaves what exists
// alone, and replaces an index that an interrupted build left invalid.
func buildTaskIndexes(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	parts, err := partitionsOfTasks(ctx, conn)
	if err != nil {
		return err
	}
	for _, part := range parts {
		for _, ix := range taskIndexes {
			name := pgx.Identifier{part + "_" + ix.suffix}.Sanitize()
			var attached bool
			if err := conn.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_index x
					JOIN pg_inherits ih ON ih.inhrelid = x.indexrelid
					WHERE ih.inhparent = to_regclass($1) AND x.indrelid = to_regclass($2) AND x.indisvalid)`,
				ix.parent, pgx.Identifier{part}.Sanitize()).Scan(&attached); err != nil {
				return err
			}
			if attached {
				continue
			}
			for _, stmt := range []string{
				"DROP INDEX CONCURRENTLY IF EXISTS " + name,
				"CREATE INDEX CONCURRENTLY " + name + " ON " + pgx.Identifier{part}.Sanitize() + " " + ix.columns,
				"ALTER INDEX " + ix.parent + " ATTACH PARTITION " + name,
			} {
				if _, err := conn.ExecContext(ctx, stmt); err != nil {
					return fmt.Errorf("%s: %w", stmt, err)
				}
			}
		}
	}
	return nil
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
