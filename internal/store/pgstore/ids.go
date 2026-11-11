package pgstore

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"sync"
	"time"

	"github.com/cespare/xxhash/v2"

	"github.com/edevhub/crawly/internal/store"
)

// buckets is the number of buckets queue keys hash into. A bucket routes a lookup
// and, later, a shard.
const buckets = 256

// bucketOf is the bucket of a queue key: a fixed hash, so every replica agrees.
func bucketOf(key store.QueueKey) uint8 {
	return uint8(xxhash.Sum64String(string(key)) % buckets)
}

const secondsPerDay = 24 * 60 * 60

// dayOf is the number of the day t falls on, counted from the Unix epoch in UTC.
func dayOf(t time.Time) uint16 {
	return uint16(t.Unix() / secondsPerDay) //nolint:gosec // G115: overflows in the year 2149
}

// dateOf is the UTC date of a day number.
func dateOf(day uint16) time.Time {
	return time.Unix(int64(day)*secondsPerDay, 0).UTC()
}

const (
	taskKeyLen  = 1 + 2 + 16
	leaseKeyLen = taskKeyLen + 4 + 8
)

// taskKey is the decoded form of a task ID: bucket ‖ run creation day ‖ UUIDv7. The
// day routes a lookup to one partition.
type taskKey struct {
	bucket uint8
	day    uint16
	id     [16]byte
}

func (k taskKey) append(b []byte) []byte {
	b = append(b, k.bucket)
	b = binary.BigEndian.AppendUint16(b, k.day)
	return append(b, k.id[:]...)
}

func (k taskKey) ID() store.TaskID {
	return store.TaskID(base64.RawURLEncoding.EncodeToString(k.append(make([]byte, 0, taskKeyLen))))
}

func (k taskKey) date() time.Time { return dateOf(k.day) }

func decodeTask(b []byte) taskKey {
	k := taskKey{bucket: b[0], day: binary.BigEndian.Uint16(b[1:3])}
	copy(k.id[:], b[3:taskKeyLen])
	return k
}

func parseTask(id store.TaskID) (taskKey, bool) {
	b, err := base64.RawURLEncoding.DecodeString(string(id))
	if err != nil || len(b) != taskKeyLen {
		return taskKey{}, false
	}
	return decodeTask(b), true
}

// leaseKey is the decoded form of a lease ID: task ‖ attempt ‖ nonce. The nonce makes
// a lease ID unique per dispatch even when an attempt number is used again.
type leaseKey struct {
	task    taskKey
	attempt uint32
	nonce   int64
}

func (l leaseKey) ID() store.LeaseID {
	b := l.task.append(make([]byte, 0, leaseKeyLen))
	b = binary.BigEndian.AppendUint32(b, l.attempt)
	b = binary.BigEndian.AppendUint64(b, uint64(l.nonce)) //nolint:gosec // G115: the nonce is 64 opaque bits
	return store.LeaseID(base64.RawURLEncoding.EncodeToString(b))
}

func (l leaseKey) ref() store.LeaseRef { return store.LeaseRef{ID: l.ID(), Attempt: l.attempt} }

func parseLease(id store.LeaseID) (leaseKey, bool) {
	b, err := base64.RawURLEncoding.DecodeString(string(id))
	if err != nil || len(b) != leaseKeyLen {
		return leaseKey{}, false
	}
	return leaseKey{
		task:    decodeTask(b),
		attempt: binary.BigEndian.Uint32(b[taskKeyLen:]),
		nonce:   int64(binary.BigEndian.Uint64(b[taskKeyLen+4:])), //nolint:gosec // G115: the nonce is 64 opaque bits
	}, true
}

func newNonce() int64 {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return int64(binary.BigEndian.Uint64(b[:])) //nolint:gosec // G115: the nonce is 64 opaque bits
}

// idGen produces UUIDv7 values that increase within the process, so tasks of one
// batch keep their order.
type idGen struct {
	mu  sync.Mutex
	ms  int64
	seq uint16
}

const maxSeq = 0x0fff // the 12 bits UUIDv7 leaves next to the timestamp

func (g *idGen) next() [16]byte {
	g.mu.Lock()
	if ms := time.Now().UnixMilli(); ms > g.ms {
		g.ms, g.seq = ms, 0
	} else {
		// The clock did not move: count on, and borrow the next millisecond when the
		// sequence is full.
		g.seq++
		if g.seq > maxSeq {
			g.ms, g.seq = g.ms+1, 0
		}
	}
	ms, seq := g.ms, g.seq
	g.mu.Unlock()

	var id [16]byte
	_, _ = rand.Read(id[8:])
	var ts [8]byte
	binary.BigEndian.PutUint64(ts[:], uint64(ms)) //nolint:gosec // G115: milliseconds since the epoch are positive
	copy(id[:6], ts[2:])
	id[6] = 0x70 | byte(seq>>8)
	id[7] = byte(seq & 0xff)
	id[8] = 0x80 | id[8]&0x3f
	return id
}
