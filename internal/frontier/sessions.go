package frontier

import (
	"cmp"
	"context"
	"fmt"
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
	slices.SortFunc(out.Sessions, func(a, b *sessionv1.Session) int { return cmp.Compare(sortKey(a), sortKey(b)) })
	// The token is the sort key of the last session of the previous page.
	if after := req.GetPageToken(); after != "" {
		i, _ := slices.BinarySearchFunc(out.Sessions, after, func(s *sessionv1.Session, key string) int {
			if k := sortKey(s); k <= key {
				return -1
			}
			return 1
		})
		out.Sessions = out.Sessions[i:]
	}
	size := int(req.GetPageSize())
	if size <= 0 || size > maxSessionsPage {
		size = maxSessionsPage
	}
	if len(out.Sessions) > size {
		out.NextPageToken = sortKey(out.Sessions[size-1])
		out.Sessions = out.Sessions[:size]
	}
	return out, nil
}

// maxSessionsPage is the most sessions one ListSessions call returns.
const maxSessionsPage = 1000

// sortKey orders sessions by pool, then connection time, then id.
func sortKey(s *sessionv1.Session) string {
	return fmt.Sprintf("%s\x00%020d\x00%s", s.GetPool(), s.GetConnectedTime().AsTime().UnixNano(), s.GetSessionId())
}
