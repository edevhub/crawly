package pgstore

import (
	"context"
	"slices"

	"github.com/jackc/pgx/v5"

	"github.com/edevhub/crawly/internal/store"
)

// Task labels (ADR 0002). run_labels is the registry of the labels a run has accepted;
// a new label is registered under the run's label lock, which makes the cap exact.
// The counters are kept per queue, in backlog_labels, like the other counters of a
// backlog: a Finish updates the row of its own queue, which it holds, so that no
// counter is shared by all queues of a run. They are summed on read.

// knownLabels returns the labels of the run that the batch can use: those it has
// already, and the room for the new ones. It is nil for a batch without labels.
// When the batch brings a label the run does not have and the run has room for it,
// the run's label lock is taken
// and held to the end of the transaction, so that batches that bring new labels
// count against the cap one after the other.
func (s *Store) knownLabels(ctx context.Context, tx pgx.Tx, m *runMeta, tasks []store.NewTask, limit int) (map[string]bool, error) {
	var wanted []string
	for i := range tasks {
		if l := tasks[i].Label; l != "" && !tasks[i].Invalid {
			wanted = append(wanted, l)
		}
	}
	if len(wanted) == 0 {
		return nil, nil //nolint:nilnil // nil is "no labels in the batch", and a nil map reads as empty
	}
	known, err := loadLabels(ctx, tx, m.no)
	if err != nil {
		return nil, err
	}
	if !slices.ContainsFunc(wanted, func(l string) bool { return !known[l] }) {
		return known, nil
	}
	if limit > 0 && len(known) >= limit {
		// The run is full and labels are never removed: every new label is rejected,
		// whatever another batch does, so there is nothing to count and no lock to take.
		return known, nil
	}
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1::int, $2::int)", lockLabels, lockKey(m.no)); err != nil {
		return nil, err
	}
	return loadLabels(ctx, tx, m.no)
}

func loadLabels(ctx context.Context, q querier, run int64) (map[string]bool, error) {
	rows, err := q.Query(ctx, "SELECT label FROM run_labels WHERE run_no = $1", run)
	if err != nil {
		return nil, err
	}
	labels, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, err
	}
	known := make(map[string]bool, len(labels)+1)
	for _, l := range labels {
		known[l] = true
	}
	return known, nil
}

// registerLabels records labels of tasks that were inserted.
func registerLabels(ctx context.Context, tx pgx.Tx, run int64, labels []string) error {
	if len(labels) == 0 {
		return nil
	}
	_, err := tx.Exec(ctx, `INSERT INTO run_labels (run_no, label) SELECT $1, l FROM unnest($2::text[]) AS u(l) ORDER BY l
		ON CONFLICT DO NOTHING`, run, labels)
	return err
}

// countLabel adds n tasks that ended in the state the column names to the counters of
// a label. The column is one of a fixed set, never input.
func countLabel(batch *pgx.Batch, queue string, run int64, label, column string, n int64) {
	if label == "" || n == 0 {
		return
	}
	batch.Queue(`INSERT INTO backlog_labels (queue_key, run_no, label, `+column+`) VALUES ($1, $2, $3, $4)
		ON CONFLICT (queue_key, run_no, label) DO UPDATE SET `+column+` = backlog_labels.`+column+` + EXCLUDED.`+column,
		queue, run, label, n)
}

// countLabels does the same for several labels, in key order.
func countLabels(batch *pgx.Batch, queue string, run int64, column string, counts map[string]int64) {
	labels := make([]string, 0, len(counts))
	for l := range counts {
		labels = append(labels, l)
	}
	slices.Sort(labels)
	for _, l := range labels {
		countLabel(batch, queue, run, l, column, counts[l])
	}
}

// countFailed counts a failed task of a label, with the code of its last attempt.
func countFailed(batch *pgx.Batch, queue string, run int64, label, code string) {
	if code == "" {
		countLabel(batch, queue, run, label, "failed", 1)
		return
	}
	if label == "" {
		return
	}
	batch.Queue(`INSERT INTO backlog_labels (queue_key, run_no, label, failed, failed_codes)
		VALUES ($1, $2, $3, 1, jsonb_build_object($4::text, 1))
		ON CONFLICT (queue_key, run_no, label) DO UPDATE SET failed = backlog_labels.failed + 1,
			failed_codes = jsonb_set(backlog_labels.failed_codes, ARRAY[$4::text],
				to_jsonb(coalesce((backlog_labels.failed_codes ->> $4)::bigint, 0) + 1))`,
		queue, run, label, code)
}

// labelStats sums the counters of a run's labels. Every registered label is listed.
func labelStats(ctx context.Context, q querier, run int64) (map[string]store.LabelStats, error) {
	rows, err := q.Query(ctx, `SELECT r.label, coalesce(sum(b.succeeded), 0)::bigint, coalesce(sum(b.failed), 0)::bigint,
			coalesce(sum(b.expired), 0)::bigint, coalesce(sum(b.cancelled), 0)::bigint
		FROM run_labels r LEFT JOIN backlog_labels b ON b.run_no = r.run_no AND b.label = r.label
		WHERE r.run_no = $1 GROUP BY r.label`, run)
	if err != nil {
		return nil, err
	}
	var label string
	var ok, failed, expired, cancelled int64
	var out map[string]store.LabelStats
	if _, err := pgx.ForEachRow(rows, []any{&label, &ok, &failed, &expired, &cancelled}, func() error {
		if out == nil {
			out = map[string]store.LabelStats{}
		}
		out[label] = store.LabelStats{Succeeded: uint64(ok), Failed: uint64(failed), Expired: uint64(expired), //nolint:gosec // G115: counters are not negative
			Cancelled: uint64(cancelled), FailedByCode: map[string]uint64{}} //nolint:gosec // G115: counters are not negative
		return nil
	}); err != nil || out == nil {
		return nil, err
	}
	rows, err = q.Query(ctx, `SELECT label, c.key, sum(c.value::bigint)::bigint FROM backlog_labels b, jsonb_each_text(b.failed_codes) c
		WHERE b.run_no = $1 GROUP BY label, c.key`, run)
	if err != nil {
		return nil, err
	}
	var code string
	var n int64
	_, err = pgx.ForEachRow(rows, []any{&label, &code, &n}, func() error {
		if l, there := out[label]; there {
			l.FailedByCode[code] = uint64(n) //nolint:gosec // G115: counters are not negative
		}
		return nil
	})
	return out, err
}

// tally counts the tasks per label, leaving out the ones without.
func tally(labels []string) map[string]int64 {
	out := map[string]int64{}
	for _, l := range labels {
		if l != "" {
			out[l]++
		}
	}
	return out
}
