package wire

import (
	"slices"
	"testing"

	"google.golang.org/protobuf/proto"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
)

// sized is a task whose spec encodes to about n bytes.
func sized(n int) *taskv1.TaskSpec {
	return &taskv1.TaskSpec{Context: map[string][]byte{"k": make([]byte, n)}}
}

func TestBatches(t *testing.T) {
	split := func(maxTasks int, tasks ...*taskv1.TaskSpec) [][2]int {
		var out [][2]int
		Batches(tasks, maxTasks, func(from, to int) { out = append(out, [2]int{from, to}) })
		return out
	}
	half := sized(MaxBatchBytes/2 - 64)
	for name, c := range map[string]struct {
		got, want [][2]int
	}{
		"nothing":             {split(10), nil},
		"one task":            {split(10, sized(10)), [][2]int{{0, 1}}},
		"by count":            {split(2, sized(1), sized(1), sized(1), sized(1), sized(1)), [][2]int{{0, 2}, {2, 4}, {4, 5}}},
		"by size":             {split(10, half, half, half), [][2]int{{0, 2}, {2, 3}}},
		"a task at the limit": {split(10, sized(10), sized(MaxTaskBytes-64), sized(MaxTaskBytes-64), sized(MaxTaskBytes-64)), [][2]int{{0, 3}, {3, 4}}},
	} {
		if !slices.Equal(c.got, c.want) {
			t.Errorf("%s: %v, want %v", name, c.got, c.want)
		}
	}
	// No batch of several tasks weighs more than the limit.
	tasks := make([]*taskv1.TaskSpec, 50)
	for i := range tasks {
		tasks[i] = sized((i%7 + 1) * 100 << 10)
	}
	next := 0
	Batches(tasks, 200, func(from, to int) {
		size := 0
		for _, ts := range tasks[from:to] {
			size += proto.Size(ts)
		}
		if from != next || to <= from || size > MaxBatchBytes {
			t.Errorf("batch [%d,%d): %d bytes, expected to start at %d", from, to, size, next)
		}
		next = to
	})
	if next != len(tasks) {
		t.Errorf("the batches end at %d of %d tasks", next, len(tasks))
	}
}
