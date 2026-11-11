// Package policy exports the policy types of the Frontier API: the limits and behaviour
// a run can set for itself, and the read-only service that shows the global policies
// and how they resolve. The types are aliases of the generated types, so there is one
// definition of each.
package policy

import (
	"google.golang.org/grpc"

	policyv1 "github.com/edevhub/crawly/internal/gen/frontier/policy/v1"
)

// Client is the policy service client.
type Client = policyv1.PolicyServiceClient

// NewClient returns a policy service client on a connection.
func NewClient(conn grpc.ClientConnInterface) Client { return policyv1.NewPolicyServiceClient(conn) }

type (
	// Policy is a global policy: limits and behaviour for the queues its selector matches.
	Policy = policyv1.Policy
	// Selector chooses the queues a Policy applies to, by a glob over queue keys.
	Selector = policyv1.PolicySelector
	// Limits are a queue's politeness limits.
	Limits = policyv1.QueueLimits
	// AdaptiveLimits slow a queue down while its site is under strain.
	AdaptiveLimits = policyv1.AdaptiveLimits
	// Behaviour is how tasks are retried and leased: attempts, backoff and timeouts.
	Behaviour = policyv1.Behaviour
	// Backoff is the delay between retries of a task.
	Backoff = policyv1.Backoff
	// Run is a run's own policy, given at CreateRun.
	Run = policyv1.RunPolicy
	// RunOverride is a run's override for the queues a glob matches.
	RunOverride = policyv1.RunQueueOverride
)

// Requests and responses, named as in the API.
type (
	// ListPoliciesRequest is the request of the ListPolicies RPC.
	ListPoliciesRequest = policyv1.ListPoliciesRequest
	// ListPoliciesResponse is the response of the ListPolicies RPC.
	ListPoliciesResponse = policyv1.ListPoliciesResponse
	// GetPolicyRequest is the request of the GetPolicy RPC.
	GetPolicyRequest = policyv1.GetPolicyRequest
	// GetPolicyResponse is the response of the GetPolicy RPC.
	GetPolicyResponse = policyv1.GetPolicyResponse
	// ExplainPolicyRequest is the request of the ExplainPolicy RPC.
	ExplainPolicyRequest = policyv1.ExplainPolicyRequest
	// ExplainPolicyResponse is the response of the ExplainPolicy RPC.
	ExplainPolicyResponse = policyv1.ExplainPolicyResponse
)
