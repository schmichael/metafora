package mux

import (
	"math"
	"sync"
	"time"

	"github.com/lytics/metafora"
)

const (
	defkey  = ""
	TypeKey = "_type"
)

var (
	// AlwaysReject is a balancer which always ignores tasks forever. Useful to
	// switch the default balancing logic from Always Accepting to Always
	// Rejecting.
	AlwaysReject = rejecter{}

	rejectRoute = route{b: AlwaysReject, h: nil}
	forever     = time.Unix(math.MaxInt64, 0)
)

type rejecter struct{}

func (rejecter) Init(metafora.BalancerContext)            {}
func (rejecter) CanClaim(metafora.Task) (time.Time, bool) { return forever, false }
func (rejecter) Balance() []string                        { return nil }

type route struct {
	b metafora.Balancer
	h metafora.HandlerFunc
}

type mux struct {
	mu     *sync.RWMutex
	routes map[string]route
}

// New creates a new task type multiplexer. It routes tasks to appropriate
// balancers and handlers based on their _type property.
//
// Tasks without a _type property will use the default handler and balancer. To
// reject tasks without a _type property call New(nil, AlwaysReject).
//
// Tasks with an unknown _type property will be rejected.
func New(defh metafora.HandlerFunc, defb metafora.Balancer) *mux {
	return &mux{
		mu:     &sync.RWMutex{},
		routes: map[string]route{defkey: {b: defb, h: defh}},
	}
}

// Add a new type to the task multiplexer. Passing nil will use the default
// handler or balancer.
func (m *mux) Add(typ string, h metafora.HandlerFunc, b metafora.Balancer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h == nil {
		h = m.routes[defkey].h
	}
	if b == nil {
		b = m.routes[defkey].b
	}
	m.routes[typ] = route{h: h, b: b}
}

func (m *mux) get(typ string) route {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.routes[typ]
	if !ok {
		metafora.Debugf("No route for type=%q, ignoring forever.", typ)
		return rejectRoute
	}
	return r
}

// Init calls init on all registered balancers. If you reuse balancers for
// multiple types, it will call Init multiple times.
func (m *mux) Init(ctx metafora.BalancerContext) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for t, r := range m.routes {
		r.b.Init(ctx)
	}
}

// CanClaim looks up the appropriate balancer for the route and calls its
// CanClaim method.
func (m *mux) CanClaim(task metafora.Task) (time.Time, bool) {
	r := m.get(task.Props()[TypeKey])
	return r.b.CanClaim(task)
}

func (m *mux) Balance()
