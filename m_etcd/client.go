package m_etcd

import (
	"encoding/json"
	"path"
	"time"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

// NewClient creates a new client using an etcd backend.
func NewClient(namespace string, client *etcd.Client) metafora.Client {
	if namespace[0] != '/' {
		namespace = "/" + namespace
	}

	return &mclient{
		etcd:      client,
		namespace: namespace,
	}
}

// Type 'mclient' is an internal implementation of metafora.Client with an etcd backend.
type mclient struct {
	etcd      *etcd.Client
	namespace string
}

// taskPath is the etcd path to a task's props file.
func (mc *mclient) taskPath(tid string) string {
	return path.Join(mc.namespace, TasksPath, tid, propsKey)
}

// cmdPath is the path to a particular nodeId, represented as a directory in etcd.
func (mc *mclient) cmdPath(node string) string {
	return path.Join(mc.namespace, NodesPath, node, "commands")
}

// SubmitTask submits a task to an etcd coordinator. See the etcd documentation
// for the key layout.
func (mc *mclient) SubmitTask(id string, props map[string]string) error {
	if props == nil {
		props = map[string]string{}
	}
	// Add internal metadata properties and marshal props
	props["_submitted"] = time.Now().Format(time.RFC3339Nano)

	buf, err := json.Marshal(props)
	if err != nil {
		// I don't think this is even possible when marshaling map[string]strings
		return err
	}

	fullpath := mc.taskPath(id)
	_, err = mc.etcd.Create(fullpath, string(buf), ForeverTTL)
	metafora.Debugf("task=%q submitted", id)
	return err
}

// SubmitCommand creates a new command for a particular nodeId, the
// command has a random name and is added to the particular nodeId
// directory in etcd.
func (mc *mclient) SubmitCommand(node string, command metafora.Command) error {
	cmdPath := mc.cmdPath(node)
	body, err := command.Marshal()
	if err != nil {
		// This is either a bug in metafora or someone implemented their own
		// command incorrectly.
		return err
	}
	if _, err := mc.etcd.AddChild(cmdPath, string(body), ForeverTTL); err != nil {
		metafora.Errorf("Error submitting command: %s to node: %s", command, node)
		return err
	}
	metafora.Debugf("Submitted command: %s to node: %s", command, node)
	return nil
}
