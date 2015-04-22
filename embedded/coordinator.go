package embedded

import (
	"errors"
	"sync"

	"github.com/lytics/metafora"
)

type task struct {
	id    string
	props map[string]string
}

func (t *task) ID() string               { return t.id }
func (t *task) Props() map[string]string { return t.props }

// NewTask returns a metafora.Task implementation based on the given ID and
// properties. It's meant to be used by the embedded coordinator and tests.
func NewTask(id string, props map[string]string) metafora.Task {
	return &task{id: id, props: props}
}

func NewEmbeddedCoordinator(nodeid string, inchan chan metafora.Task, cmdchan chan *NodeCommand) metafora.Coordinator {
	e := &EmbeddedCoordinator{
		inchan:   inchan,
		cmdchan:  cmdchan,
		stopchan: make(chan struct{}),
		tasksL:   &sync.Mutex{},
		tasks:    make(map[string]metafora.Task),
	}

	return e
}

// Coordinator which listens for tasks on a channel
type EmbeddedCoordinator struct {
	nodeid   string
	ctx      metafora.CoordinatorContext
	inchan   chan metafora.Task
	cmdchan  chan *NodeCommand
	stopchan chan struct{}

	tasksL *sync.Mutex
	tasks  map[string]metafora.Task
}

func (e *EmbeddedCoordinator) Init(c metafora.CoordinatorContext) error {
	e.ctx = c
	return nil
}

func (e *EmbeddedCoordinator) Watch(out chan<- metafora.Task) error {
	for {
		// wait for incoming tasks
		select {
		case task, ok := <-e.inchan:
			if !ok {
				return errors.New("Input closed")
			}
			e.tasksL.Lock()
			e.tasks[task.ID()] = task
			e.tasksL.Unlock()
			select {
			case out <- task:
			case <-e.stopchan:
				return nil
			}
		case <-e.stopchan:
			return nil
		}
	}
}

func (e *EmbeddedCoordinator) Claim(taskID string) bool {
	// We recieved on a channel, we are the only ones to pull that value
	return true
}

func (e *EmbeddedCoordinator) Release(taskID string) {
	// Releasing should be async to avoid deadlocks (and better reflect the
	// behavior of "real" coordinators)
	e.tasksL.Lock()
	defer e.tasksL.Unlock()
	task, ok := e.tasks[taskID]
	if !ok {
		panic("cannot release a task that has never been seen! " + taskID)
	}
	go func() {
		select {
		case e.inchan <- task:
		case <-e.stopchan:
		}
	}()
}

func (e *EmbeddedCoordinator) Done(taskID string) {
	e.tasksL.Lock()
	defer e.tasksL.Unlock()
	delete(e.tasks, taskID)
}

func (e *EmbeddedCoordinator) Command() (metafora.Command, error) {
	select {
	case cmd, ok := <-e.cmdchan:
		if !ok {
			return nil, errors.New("Cmd channel closed")
		}
		return cmd.Cmd, nil
	case <-e.stopchan:
		return nil, nil
	}
}

func (e *EmbeddedCoordinator) Close() {
	close(e.stopchan)
}
