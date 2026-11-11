// Package policy loads the static global policies and resolves them, together with a
// run's policy, into the limits and behaviour that apply to one run on one queue.
//
// Policies never change while a replica runs (ADR 0001): changing one is a deploy.
package policy

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	policyv1 "github.com/edevhub/crawly/internal/gen/frontier/policy/v1"
	"github.com/edevhub/crawly/internal/store"
)

// builtin is the provenance name of values no policy set.
const builtin = "builtin"

// Defaults used when no policy sets a field. They are deliberately conservative.
var (
	defaultLimits = &policyv1.QueueLimits{
		MaxConcurrency: proto.Uint32(2),
		RatePerSecond:  proto.Float64(1),
		Burst:          proto.Uint32(1),
	}
	defaultBehaviour = &policyv1.Behaviour{
		MaxAttempts: proto.Uint32(3),
		Backoff: &policyv1.Backoff{
			Initial:    durationpb.New(2 * time.Second),
			Multiplier: proto.Float64(2),
			Max:        durationpb.New(5 * time.Minute),
			Jitter:     new(0.2),
		},
		FetchTimeout:     durationpb.New(30 * time.Second),
		LeaseDuration:    durationpb.New(60 * time.Second),
		MaxLeaseDuration: durationpb.New(10 * time.Minute),
		MaxQueueBlock:    durationpb.New(24 * time.Hour),
	}
)

// Set is an immutable set of global policies.
type Set struct {
	policies []*policyv1.Policy // least specific first
	hash     string
	cache    sync.Map // cacheKey -> store.Effective
	cached   int64
	mu       sync.Mutex
}

// New validates the policies and builds a set.
func New(policies []*policyv1.Policy) (*Set, error) {
	seen := map[string]bool{}
	ps := make([]*policyv1.Policy, 0, len(policies))
	for _, p := range policies {
		if p.GetName() == "" {
			return nil, errors.New("policy: a policy has no name")
		}
		if p.GetName() == builtin {
			return nil, fmt.Errorf("policy: the name %q is reserved", builtin)
		}
		if seen[p.GetName()] {
			return nil, fmt.Errorf("policy: duplicate policy %q", p.GetName())
		}
		seen[p.GetName()] = true
		if err := validateLimits(p.GetLimits()); err != nil {
			return nil, fmt.Errorf("policy %q: %w", p.GetName(), err)
		}
		if b := p.GetBehaviour(); b.GetMaxQueueBlock() != nil && b.GetMaxQueueBlock().AsDuration() <= 0 {
			// Zero would silently turn off every block of the queue, a site's
			// Retry-After included.
			return nil, fmt.Errorf("policy %q: max_queue_block must be positive", p.GetName())
		}
		ps = append(ps, proto.CloneOf(p))
	}
	slices.SortStableFunc(ps, func(a, b *policyv1.Policy) int {
		return cmp.Or(
			cmp.Compare(specificity(a.GetMatch().GetQueueKey()), specificity(b.GetMatch().GetQueueKey())),
			cmp.Compare(a.GetPrecedence(), b.GetPrecedence()),
			cmp.Compare(a.GetName(), b.GetName()),
		)
	})
	h := sha256.New()
	for _, p := range ps {
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(p)
		if err != nil {
			return nil, err
		}
		_, _ = fmt.Fprintf(h, "%d:", len(b)) // a hash.Hash never fails to write
		h.Write(b)
	}
	return &Set{policies: ps, hash: hex.EncodeToString(h.Sum(nil))[:16]}, nil
}

// Load reads a JSON file of the form {"policies": [ ... ]}, where each entry is a
// Policy message in proto JSON (durations as "30s").
func Load(path string) (*Set, error) {
	data, err := os.ReadFile(path) //nolint:gosec // G304: the policy file is chosen by the operator, on the command line
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

// Parse is Load for bytes.
func Parse(data []byte) (*Set, error) {
	var file struct {
		Policies []json.RawMessage `json:"policies"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("policy: %w", err)
	}
	ps := make([]*policyv1.Policy, 0, len(file.Policies))
	for i, raw := range file.Policies {
		p := &policyv1.Policy{}
		if err := protojson.Unmarshal(raw, p); err != nil {
			return nil, fmt.Errorf("policy: entry %d: %w", i, err)
		}
		ps = append(ps, p)
	}
	return New(ps)
}

// Hash identifies the policy set.
func (s *Set) Hash() string { return s.hash }

// Policies returns the loaded policies, least specific first.
func (s *Set) Policies() []*policyv1.Policy { return s.policies }

// Get returns one policy by name.
func (s *Set) Get(name string) (*policyv1.Policy, bool) {
	if i := slices.IndexFunc(s.policies, func(p *policyv1.Policy) bool { return p.GetName() == name }); i >= 0 {
		return s.policies[i], true
	}
	return nil, false
}

// Resolved is the outcome of resolution for one run on one queue.
type Resolved struct {
	Limits     *policyv1.QueueLimits // what the run's tasks are admitted against
	Queue      *policyv1.QueueLimits // the queue's shared limits (global policies only)
	Own        bool                  // the run set limits of its own for this queue
	Weight     uint32
	Behaviour  *policyv1.Behaviour
	Provenance map[string]string // field path -> name of the policy or override that set it
}

// Resolve applies, least specific first: built-in defaults, matching global policies
// (by glob specificity, then precedence), the run policy, the most specific matching
// run override. The most specific value wins, field by field.
func (s *Set) Resolve(rp *policyv1.RunPolicy, queue string) Resolved {
	r := Resolved{
		Limits:     &policyv1.QueueLimits{},
		Behaviour:  &policyv1.Behaviour{Backoff: &policyv1.Backoff{}},
		Weight:     1,
		Provenance: map[string]string{"weight": builtin},
	}
	mergeLimits(r.Limits, defaultLimits, builtin, r.Provenance)
	mergeBehaviour(r.Behaviour, defaultBehaviour, builtin, r.Provenance)
	for _, p := range s.policies {
		if !Match(p.GetMatch().GetQueueKey(), queue) {
			continue
		}
		mergeLimits(r.Limits, p.GetLimits(), p.GetName(), r.Provenance)
		mergeBehaviour(r.Behaviour, p.GetBehaviour(), p.GetName(), r.Provenance)
	}
	r.Queue = proto.CloneOf(r.Limits)
	if rp == nil {
		return r
	}
	// A block stops every run on the queue, so its ceiling is the queue's: what a run
	// says about it does not apply.
	const blockField = "behaviour.max_queue_block"
	ceiling, ceilingFrom := r.Behaviour.GetMaxQueueBlock(), r.Provenance[blockField]
	defer func() { r.Behaviour.MaxQueueBlock, r.Provenance[blockField] = ceiling, ceilingFrom }()
	if rp.Weight != nil {
		r.Weight, r.Provenance["weight"] = rp.GetWeight(), "run"
	}
	if rp.GetLimits() != nil {
		r.Own = true
		mergeLimits(r.Limits, rp.GetLimits(), "run", r.Provenance)
	}
	mergeBehaviour(r.Behaviour, rp.GetBehaviour(), "run", r.Provenance)
	var best *policyv1.RunQueueOverride
	for _, o := range rp.GetOverrides() {
		if Match(o.GetQueueKey(), queue) && (best == nil || specificity(o.GetQueueKey()) > specificity(best.GetQueueKey())) {
			best = o
		}
	}
	if best != nil {
		name := "run override " + best.GetQueueKey()
		if best.Weight != nil {
			r.Weight, r.Provenance["weight"] = best.GetWeight(), name
		}
		if best.GetLimits() != nil {
			r.Own = true
			mergeLimits(r.Limits, best.GetLimits(), name, r.Provenance)
		}
		mergeBehaviour(r.Behaviour, best.GetBehaviour(), name, r.Provenance)
	}
	if r.Weight == 0 {
		r.Weight = 1
	}
	return r
}

type cacheKey struct {
	run   store.RunID
	queue store.QueueKey
}

// For implements store.Policies. Results are cached: a run's policy never changes.
func (s *Set) For(run store.RunID, rp *policyv1.RunPolicy, queue store.QueueKey) store.Effective {
	k := cacheKey{run, queue}
	if v, ok := s.cache.Load(k); ok {
		if e, ok := v.(store.Effective); ok {
			return e
		}
	}
	r := s.Resolve(rp, string(queue))
	e := store.Effective{
		Limits: ToLimits(r.Limits),
		Own:    r.Own,
		Queue:  ToLimits(r.Queue),
		Weight: float64(r.Weight),
		Lease: store.LeaseSettings{
			Duration:    r.Behaviour.GetLeaseDuration().AsDuration(),
			MaxDuration: r.Behaviour.GetMaxLeaseDuration().AsDuration(),
		},
	}
	s.mu.Lock()
	if s.cached >= 200_000 { // bounded: resolution is pure and can be redone
		s.cache.Clear()
		s.cached = 0
	}
	s.cached++
	s.mu.Unlock()
	s.cache.Store(k, e)
	return e
}

// ToLimits converts API limits to GCRA form: T = max(1/rate, min_delay).
func ToLimits(l *policyv1.QueueLimits) store.Limits {
	out := store.Limits{Burst: 1, MaxConcurrency: int(l.GetMaxConcurrency())}
	if l.GetRatePerSecond() > 0 {
		out.Interval = time.Duration(float64(time.Second) / l.GetRatePerSecond())
	}
	if d := l.GetMinDelay().AsDuration(); d > out.Interval {
		out.Interval = d
	}
	if l.GetBurst() > 1 {
		out.Burst = int(l.GetBurst())
	}
	return out
}

// Match reports whether a glob matches a queue key. "*" matches any run of characters;
// an empty pattern matches everything.
func Match(pattern, key string) bool {
	if pattern == "" {
		return true
	}
	parts := strings.Split(pattern, "*")
	if len(parts) == 1 {
		return pattern == key
	}
	if !strings.HasPrefix(key, parts[0]) {
		return false
	}
	key = key[len(parts[0]):]
	last := parts[len(parts)-1]
	for _, mid := range parts[1 : len(parts)-1] {
		i := strings.Index(key, mid)
		if i < 0 {
			return false
		}
		key = key[i+len(mid):]
	}
	return strings.HasSuffix(key, last)
}

// specificity orders globs: more literal characters is more specific, and a pattern
// without a wildcard beats any with one.
func specificity(pattern string) int {
	n := len(strings.ReplaceAll(pattern, "*", ""))
	if pattern != "" && !strings.Contains(pattern, "*") {
		n += 1 << 20
	}
	return n
}

// ValidateRun checks a run's own policy for what a run may not set.
func ValidateRun(rp *policyv1.RunPolicy) error {
	if rp.GetBehaviour().GetMaxQueueBlock() != nil {
		return errors.New("behaviour.max_queue_block cannot be set by a run: a block stops every run on the queue, so its ceiling comes from the global policies")
	}
	for _, o := range rp.GetOverrides() {
		if o.GetBehaviour().GetMaxQueueBlock() != nil {
			return fmt.Errorf("override %q: behaviour.max_queue_block cannot be set by a run", o.GetQueueKey())
		}
	}
	return nil
}

// minRate is the smallest rate_per_second accepted: one request in ten years.
const minRate = 1.0 / (10 * 365 * 24 * 3600)

func validateLimits(l *policyv1.QueueLimits) error {
	if l == nil {
		return nil
	}
	if l.RatePerSecond != nil && l.GetRatePerSecond() < 0 {
		return errors.New("rate_per_second must not be negative")
	}
	// 1/rate must fit in a time.Duration: a rate too small would overflow into "no
	// limit" on some platforms.
	if r := l.GetRatePerSecond(); r > 0 && r < minRate {
		return fmt.Errorf("rate_per_second must be at least %g (one request in ten years); use a queue block to stop a queue", minRate)
	}
	if l.GetMinDelay().AsDuration() < 0 {
		return errors.New("min_delay must not be negative")
	}
	return nil
}

func mergeLimits(dst, src *policyv1.QueueLimits, name string, prov map[string]string) {
	if src == nil {
		return
	}
	if src.MaxConcurrency != nil {
		dst.MaxConcurrency, prov["limits.max_concurrency"] = new(src.GetMaxConcurrency()), name
	}
	if src.RatePerSecond != nil {
		dst.RatePerSecond, prov["limits.rate_per_second"] = new(src.GetRatePerSecond()), name
	}
	if src.Burst != nil {
		dst.Burst, prov["limits.burst"] = new(src.GetBurst()), name
	}
	if src.MinDelay != nil {
		dst.MinDelay, prov["limits.min_delay"] = src.MinDelay, name
	}
}

func mergeBehaviour(dst, src *policyv1.Behaviour, name string, prov map[string]string) {
	if src == nil {
		return
	}
	if src.MaxAttempts != nil {
		dst.MaxAttempts, prov["behaviour.max_attempts"] = new(src.GetMaxAttempts()), name
	}
	if b := src.GetBackoff(); b != nil {
		if b.Initial != nil {
			dst.Backoff.Initial, prov["behaviour.backoff.initial"] = b.Initial, name
		}
		if b.Multiplier != nil {
			dst.Backoff.Multiplier, prov["behaviour.backoff.multiplier"] = new(b.GetMultiplier()), name
		}
		if b.Max != nil {
			dst.Backoff.Max, prov["behaviour.backoff.max"] = b.Max, name
		}
		if b.Jitter != nil {
			dst.Backoff.Jitter, prov["behaviour.backoff.jitter"] = new(b.GetJitter()), name
		}
	}
	if src.FetchTimeout != nil {
		dst.FetchTimeout, prov["behaviour.fetch_timeout"] = src.FetchTimeout, name
	}
	if src.LeaseDuration != nil {
		dst.LeaseDuration, prov["behaviour.lease_duration"] = src.LeaseDuration, name
	}
	if src.MaxLeaseDuration != nil {
		dst.MaxLeaseDuration, prov["behaviour.max_lease_duration"] = src.MaxLeaseDuration, name
	}
	if src.TaskTtl != nil {
		dst.TaskTtl, prov["behaviour.task_ttl"] = src.TaskTtl, name
	}
	if src.MaxQueueBlock != nil {
		dst.MaxQueueBlock, prov["behaviour.max_queue_block"] = src.MaxQueueBlock, name
	}
}
