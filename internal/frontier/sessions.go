package frontier

import (
	"cmp"
	"context"
	"slices"

	"google.golang.org/protobuf/types/known/timestamppb"

	sessionv1 "github.com/edevhub/crawly/internal/gen/frontier/session/v1"
	"github.com/edevhub/crawly/internal/safeconv"
)

type sessionService struct {
	sessionv1.UnimplementedSessionServiceServer
	s *Server
}

// ListSessions lists the sessions this replica holds. A session lives in the memory of
// the replica that serves its stream, so the answer is that replica's.
func (ss sessionService) ListSessions(_ context.Context, req *sessionv1.ListSessionsRequest) (*sessionv1.ListSessionsResponse, error) {
	s := ss.s
	s.mu.Lock()
	pools := make([]*pool, 0, len(s.pools))
	for _, p := range s.pools {
		if req.GetPool() == "" || string(p.name) == req.GetPool() {
			pools = append(pools, p)
		}
	}
	s.mu.Unlock()
	out := &sessionv1.ListSessionsResponse{ReplicaId: s.cfg.ReplicaID}
	for _, p := range pools {
		p.mu.Lock()
		for sess := range p.sessions {
			out.Sessions = append(out.Sessions, &sessionv1.Session{
				SessionId: string(sess.id), WorkerId: sess.worker.ID, BuildVersion: sess.worker.Version, Pool: string(p.name),
				ConnectedTime: timestamppb.New(sess.since), Credits: safeconv.Uint32(sess.credits),
				UnfinishedAssignments: safeconv.Uint32(len(sess.leases)), ReplicaId: s.cfg.ReplicaID})
		}
		p.mu.Unlock()
	}
	slices.SortFunc(out.Sessions, func(a, b *sessionv1.Session) int {
		return cmp.Or(cmp.Compare(a.GetPool(), b.GetPool()), a.GetConnectedTime().AsTime().Compare(b.GetConnectedTime().AsTime()),
			cmp.Compare(a.GetSessionId(), b.GetSessionId()))
	})
	return out, nil
}
