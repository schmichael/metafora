package m_etcd

import (
	"encoding/json"
	"path"
	"strings"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

const (
	// etcd/Response.Action values
	actionCreated = "create"
	actionSet     = "set"
	actionExpire  = "expire"
	actionDelete  = "delete"
	actionCAD     = "compareAndDelete"

	// claimMarker is the name of the key marking a task as claimed
	claimMarker = "owner"

	// propsKey is the name of the key containing properties
	propsKey = "props"
)

var (
	// etcd actions signifying a claim key was released
	releaseActions = map[string]bool{
		actionExpire: true,
		actionDelete: true,
		actionCAD:    true,
	}

	// etcd actions signifying a new key
	newActions = map[string]bool{
		actionCreated: true,
		actionSet:     true,
	}
)

type task struct {
	id    string
	props map[string]string
}

func (t *task) ID() string               { return t.id }
func (t *task) Props() map[string]string { return t.props }

// etcdGetter defines the specific subset of etcd.Client required by parseTask
// to make testing easier.
type etcdGetter interface {
	Get(key string, _ bool, _ bool) (*etcd.Response, error)
}

// parseTask creates a new task from an etcd response or returns false.
func parseTask(c etcdGetter, resp *etcd.Response, basePath string) (*task, bool) {
	// Sanity check / test path invariant
	if !strings.HasPrefix(resp.Node.Key, basePath) {
		metafora.Errorf("Received task from outside task path: %s", resp.Node.Key)
		return nil, false
	}

	key := strings.Trim(resp.Node.Key, "/") // strip leading and trailing /s
	parts := strings.Split(key, "/")

	// Pickup new tasks from directory events
	if newActions[resp.Action] && len(parts) == 3 && resp.Node.Dir {
		return findProps(&task{id: parts[2]}, resp.Node.Nodes)
	}

	// Pickup new tasks from props file
	if newActions[resp.Action] && len(parts) == 4 && !resp.Node.Dir && parts[3] == propsKey {
		// Unmarshal properties
		newtask := task{id: parts[2]}
		err := json.Unmarshal([]byte(resp.Node.Value), &newtask.props)
		if err != nil {
			metafora.Errorf("Error unmarshalling props for task=%q: %v", newtask.id, err)
			return nil, false
		}
		return &newtask, true
	}

	// If a claim key is removed, get the properties and return the task if it's
	// not reclaimed already
	if releaseActions[resp.Action] && len(parts) == 4 && parts[3] == claimMarker {
		newtask := task{id: parts[2]}

		const sorted = false
		const recursive = true
		resp, err := c.Get("/"+path.Join(parts[0], parts[1], parts[2]), sorted, recursive)
		if err != nil {
			metafora.Errorf("Failed retrieving properties for released task=%q: %v", newtask.id, err)
			return nil, false
		}
		return findProps(&newtask, resp.Node.Nodes)
	}

	// Ignore any other key events (_metafora keys, task deletion, etc.)
	return nil, false
}

// findProps iterates over a list of etcd nodes and loads any property files it
// finds. If it discovers a claim marker it short circuits with a failure as
// the task is inclaimable.
func findProps(task *task, nodes etcd.Nodes) (*task, bool) {
	for _, node := range nodes {
		if strings.HasSuffix(node.Key, claimMarker) {
			metafora.Debugf("Ignoring task=%q as it's already reclaimed.", task.id)
			return nil, false
		}
		if strings.HasSuffix(node.Key, propsKey) {
			err := json.Unmarshal([]byte(node.Value), &task.props)
			if err != nil {
				metafora.Errorf("Error unmarshalling props for task=%q: %v", task.id, err)
				return nil, false
			}
		}
	}
	metafora.Debugf("Received task=%q", task.id)
	return task, true
}
