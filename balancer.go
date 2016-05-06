package metafora

import (
	"math"
	"math/rand"
	"time"
)

const (
	// Default threshold is 120% of cluster average
	defaultThreshold float64 = 1.2
)

// NoDelay is simply the zero value for time and meant to be a more meaningful
// value for CanClaim methods to return instead of initializing a new empty
// time struct.
var NoDelay = time.Time{}

// BalancerContext is a limited interface exposed to Balancers from the
// Consumer for access to limited Consumer state.
type BalancerContext interface {
	// Tasks returns a sorted list of task IDs owned by this Consumer. The
	// Consumer stops task manipulations during claiming and balancing, so the
	// list will be accurate unless a task naturally completes.
	Tasks() []RunningTask
}

// Balancer is the core task balancing interface. Without a master Metafora
// clusters are cooperatively balanced -- meaning each node needs to know how
// to balance itself.
type Balancer interface {
	// CanClaim should return true if the consumer should accept a task.
	//
	// When denying a claim by returning false, CanClaim should return the time
	// at which to reconsider the task for claiming.
	CanClaim(ctx BalancerContext, task Task) (ignoreUntil time.Time, claim bool)

	// Balance should return the list of Task IDs that should be released. The
	// criteria used to determine which tasks should be released is left up to
	// the implementation.
	Balance(ctx BalancerContext) (release []string)
}

// DumbBalancer is the simplest possible balancer implementation which simply
// accepts all tasks. Since it has no state a single global instance exists.
var DumbBalancer = dumbBalancer{}

type dumbBalancer struct{}

// CanClaim always returns true.
func (dumbBalancer) CanClaim(BalancerContext, Task) (time.Time, bool) { return NoDelay, true }

// Balance never returns any tasks to balance.
func (dumbBalancer) Balance(BalancerContext) []string { return nil }

// Provides information about the cluster to be used by FairBalancer
type ClusterState interface {
	// Provide the current number of jobs
	NodeTaskCount() (map[string]int, error)
}

// NewDefaultFairBalancer creates a new FairBalancer but requires a
// ClusterState implementation to gain more information about the cluster than
// BalancerContext provides.
func NewDefaultFairBalancer(nodeid string, cs ClusterState) Balancer {
	return NewDefaultFairBalancerWithThreshold(nodeid, cs, defaultThreshold)
}

// NewDefaultFairBalancerWithThreshold allows callers to override
// FairBalancer's default 120% task load release threshold.
func NewDefaultFairBalancerWithThreshold(nodeid string, cs ClusterState, threshold float64) Balancer {
	return &FairBalancer{
		nodeid:           nodeid,
		clusterstate:     cs,
		releaseThreshold: threshold,
	}
}

// An implementation of Balancer which attempts to randomly release tasks in
// the case when the count of those currently running on this node is greater
// than some percentage of the cluster average (default 120%).
//
// This balancer will claim all tasks which were not released on the last call
// to Balance.
type FairBalancer struct {
	nodeid string

	clusterstate ClusterState

	releaseThreshold float64
	delay            time.Time
}

// CanClaim rejects tasks for a period of time if the last balance released
// tasks. Otherwise all tasks are accepted.
func (e *FairBalancer) CanClaim(_ BalancerContext, task Task) (time.Time, bool) {
	if e.delay.After(time.Now()) {
		// Return delay set by Balance()
		return e.delay, false
	}
	return NoDelay, true
}

// Balance releases tasks if this node has 120% more tasks than the average
// node in the cluster.
func (e *FairBalancer) Balance(ctx BalancerContext) []string {
	nodetasks := ctx.Tasks()

	// Reset delay
	e.delay = time.Time{}

	// If local tasks <= 1 this node should never rebalance
	if len(nodetasks) < 2 {
		return nil
	}

	current, err := e.clusterstate.NodeTaskCount()
	if err != nil {
		Warnf("Error retrieving cluster state: %v", err)
		return nil
	}

	shouldrelease := current[e.nodeid] - e.desiredCount(current)
	if shouldrelease < 1 {
		return nil
	}

	releasetasks := make([]string, 0, shouldrelease)
	releaseset := make(map[string]struct{}, shouldrelease)

	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(releasetasks) < shouldrelease {
		tid := nodetasks[random.Intn(len(nodetasks))].Task().ID()
		if _, ok := releaseset[tid]; !ok {
			releasetasks = append(releasetasks, tid)
			releaseset[tid] = struct{}{}
		}
	}

	e.delay = time.Now().Add(time.Duration(len(releasetasks)) * time.Second)
	return releasetasks
}

// Retrieve the desired maximum count, based on current cluster state
func (e *FairBalancer) desiredCount(current map[string]int) int {
	total := 0
	for _, c := range current {
		total += c
	}

	avg := 0
	if len(current) > 0 {
		avg = total / len(current)
	}

	return int(math.Ceil(float64(avg) * e.releaseThreshold))
}

// BalancerMux allows using multiple balancers as single balancer.
type BalancerMux struct {
	bals []Balancer
}

// NewBalancerMux creates a Balancer which calls each passed in Balancer in
// order. Read individual methods for behaviors.
func NewBalancerMux(b ...Balancer) Balancer {
	return BalancerMux{b}
}

// CanClaim calls CanClaim on each inner balancer returning the first delayed
// result reached.
func (b BalancerMux) CanClaim(ctx BalancerContext, task Task) (time.Time, bool) {
	for _, innerb := range b.bals {
		if t, claim := innerb.CanClaim(ctx, task); !claim {
			return t, claim
		}
	}
	return NoDelay, true
}

// Balance calls Balance on each inner balancer with a modified
// BalancerContext. The modified BalancerContext tracks what tasks each Balance
// call removes so each consecutive Balance call only gets the remaining set of
// tasks.
//
// The union of all returned tasks are then returned.
func (b BalancerMux) Balance(ctx BalancerContext) []string {
	mux := newMuxCtx(ctx)
	for _, innerb := range b.bals {
		for _, taskid := range innerb.Balance(mux) {
			mux.add(taskid)
		}
	}
	return mux.result()
}

type muxctx struct {
	remaining   []RunningTask
	releasedset map[string]struct{}
}

func newMuxCtx(ctx BalancerContext) *muxctx {
	return &muxctx{
		remaining:   ctx.Tasks(),
		releasedset: map[string]struct{}{},
	}
}

func (m *muxctx) add(taskid string) {
	m.releasedset[taskid] = struct{}{}

	// Remove task from remaining task list
	r := make([]RunningTask, len(m.remaining)-1)
	i := 0
	for _, rt := range m.remaining {
		if rt.ID() != taskid {
			r[i] = rt
			i++
		}
	}
}

func (m *muxctx) result() []string {
	r := make([]string, len(m.releasedset))
	i := 0
	for taskid := range m.releasedset {
		r[i] = taskid
		i++
	}
	return r
}

func (m *muxctx) Tasks() []RunningTask {
	return m.remaining
}
