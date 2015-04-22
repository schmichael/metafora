package metafora

import (
	"encoding/json"
	"sync"
	"time"
)

// Task is the core interface returned by Coordinators.
type Task interface {
	// ID returns the unique task identifier.
	ID() string

	// Props returns the metadata property map for the task. It should not be
	// mutated.
	//
	// Properties starting with an underscore are reserved for use by Metafora.
	Props() map[string]string
}

// RunningTask exposes extra metadata about a Task once it's running in
// metafora.
type RunningTask interface {
	Task

	// Started is the time the task started running locally.
	Started() time.Time

	// Stopped is the first time the Consumer called Stop() on the task's
	// handler (or zero if Stop hasn't been called).
	Stopped() time.Time

	json.Marshaler
}

// runningtask is the per-task state Metafora tracks internally.
type runningtask struct {
	Task

	// handler on which Run and Stop are called
	h Handler

	// stopL serializes calls to task.h.Stop() to make handler implementations
	// easier/safer as well as guard stopped
	stopL sync.Mutex

	// when task was started and when Stop was first called
	started time.Time
	stopped time.Time
}

func newTask(task Task, h Handler) *runningtask {
	return &runningtask{Task: task, h: h, started: time.Now()}
}

func (t *runningtask) stop() {
	t.stopL.Lock()
	defer t.stopL.Unlock()
	if t.stopped.IsZero() {
		t.stopped = time.Now()
	}
	t.h.Stop()
}

func (t *runningtask) Started() time.Time { return t.started }
func (t *runningtask) Stopped() time.Time {
	t.stopL.Lock()
	defer t.stopL.Unlock()
	return t.stopped
}

func (t *runningtask) MarshalJSON() ([]byte, error) {
	js := struct {
		ID      string            `json:"id"`
		Props   map[string]string `json:"props"`
		Started time.Time         `json:"started"`
		Stopped *time.Time        `json:"stopped,omitempty"`
	}{ID: t.ID(), Props: t.Props(), Started: t.started}

	// Only set stopped if it's non-zero
	if s := t.Stopped(); !s.IsZero() {
		js.Stopped = &s
	}

	return json.Marshal(&js)
}
