// Package wire holds the size limits of the messages the SDK and the Frontier exchange.
// gRPC refuses a message over 4 MiB by default; both sides keep every message well
// under that, whatever a spider puts into its tasks.
package wire

import (
	"google.golang.org/protobuf/proto"

	taskv1 "github.com/edevhub/crawly/internal/gen/frontier/task/v1"
)

const (
	// MaxTaskBytes is the largest task the Frontier accepts, as the encoded size of its
	// spec: request, headers, body and context together.
	MaxTaskBytes = 1 << 20
	// MaxBatchBytes is how much the tasks of one batch weigh at most. What the message
	// adds around them, ids and framing, is a few KiB: far from gRPC's limit.
	MaxBatchBytes = 2 << 20
)

// Batches splits tasks into consecutive batches of at most maxTasks tasks and
// MaxBatchBytes each, and calls yield with the bounds of each one. The tasks must be
// within MaxTaskBytes.
func Batches(tasks []*taskv1.TaskSpec, maxTasks int, yield func(from, to int)) {
	from, size := 0, 0
	for i, ts := range tasks {
		n := proto.Size(ts)
		if i > from && (i-from >= maxTasks || size+n > MaxBatchBytes) {
			yield(from, i)
			from, size = i, 0
		}
		size += n
	}
	if from < len(tasks) {
		yield(from, len(tasks))
	}
}
