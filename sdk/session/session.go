// Package session exports the session API of the Frontier: the service client an operator
// uses to see the workers that are connected. The types are aliases of the generated
// types, so there is one definition of each.
package session

import (
	"google.golang.org/grpc"

	sessionv1 "github.com/edevhub/crawly/internal/gen/frontier/session/v1"
)

// Client is the session service client.
type Client = sessionv1.SessionServiceClient

// NewClient returns a session service client on a connection.
func NewClient(conn grpc.ClientConnInterface) Client { return sessionv1.NewSessionServiceClient(conn) }

// Session is one open Work stream of a worker in a pool, with the replica that holds it.
type Session = sessionv1.Session

// Requests and responses, named as in the API.
type (
	// ListSessionsRequest is the request of the ListSessions RPC.
	ListSessionsRequest = sessionv1.ListSessionsRequest
	// ListSessionsResponse is the response of the ListSessions RPC. It lists the
	// sessions of the replica that answered, and names it.
	ListSessionsResponse = sessionv1.ListSessionsResponse
)
