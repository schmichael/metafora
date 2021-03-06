package m_etcd

import (
	"encoding/json"
	"errors"
	"os"
	"path"
	"strings"
	"time"

	"code.google.com/p/go-uuid/uuid"
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
)

var (
	ClaimTTL           uint64 = 120 // seconds
	DefaultNodePathTTL uint64 = 20  // seconds

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

	restartWatchError = errors.New("index too old, need to restart watch")
)

type ownerValue struct {
	Node string `json:"node"`
}

type EtcdCoordinator struct {
	Client    *etcd.Client
	cordCtx   metafora.CoordinatorContext
	namespace string
	taskPath  string

	ClaimTTL uint64 // seconds

	NodeID      string
	nodePath    string
	nodePathTTL uint64
	commandPath string

	taskManager *taskManager

	// Close() closes stop channel to signal to watchers to exit
	stop chan bool
}

func (ec *EtcdCoordinator) closed() bool {
	select {
	case <-ec.stop:
		return true
	default:
		return false
	}
}

// NewEtcdCoordinator creates a new Metafora Coordinator implementation using
// etcd as the broker. If no node ID is specified, a unique one will be
// generated.
//
// Coordinator methods will be called by the core Metafora Consumer. Calling
// Init, Close, etc. from your own code will lead to undefined behavior.
func NewEtcdCoordinator(nodeID, namespace string, client *etcd.Client) metafora.Coordinator {
	// Namespace should be an absolute path with no trailing slash
	namespace = "/" + strings.Trim(namespace, "/ ")

	if nodeID == "" {
		hn, _ := os.Hostname()
		nodeID = hn + "-" + uuid.NewRandom().String()
	}

	nodeID = strings.Trim(nodeID, "/ ")

	client.SetTransport(transport)
	client.SetConsistency(etcd.STRONG_CONSISTENCY)
	return &EtcdCoordinator{
		Client:    client,
		namespace: namespace,

		taskPath: path.Join(namespace, TasksPath),
		ClaimTTL: ClaimTTL, //default to the package constant, but allow it to be overwritten

		NodeID:      nodeID,
		nodePath:    path.Join(namespace, NodesPath, nodeID),
		nodePathTTL: DefaultNodePathTTL,
		commandPath: path.Join(namespace, NodesPath, nodeID, CommandsPath),

		stop: make(chan bool),
	}
}

// Init is called once by the consumer to provide a Logger to Coordinator
// implementations.
func (ec *EtcdCoordinator) Init(cordCtx metafora.CoordinatorContext) error {
	metafora.Debugf("Initializing coordinator with namespace: %s and etcd cluster: %s",
		ec.namespace, strings.Join(ec.Client.GetCluster(), ", "))

	ec.cordCtx = cordCtx

	ec.upsertDir(ec.namespace, ForeverTTL)
	ec.upsertDir(ec.taskPath, ForeverTTL)
	if _, err := ec.Client.CreateDir(ec.nodePath, ec.nodePathTTL); err != nil {
		return err
	}
	go ec.nodeRefresher()
	ec.upsertDir(ec.commandPath, ForeverTTL)

	ec.taskManager = newManager(cordCtx, ec.Client, ec.taskPath, ec.NodeID, ec.ClaimTTL)
	return nil
}

func (ec *EtcdCoordinator) upsertDir(path string, ttl uint64) {
	//hidden etcd key that isn't visible to ls commands on the directory,
	//  you have to know about it to find it :).  I'm using it to add some
	//  info about when the cluster's schema was setup.
	pathMarker := path + "/" + MetadataKey
	const sorted = false
	const recursive = false

	_, err := ec.Client.Get(path, sorted, recursive)
	if err == nil {
		return
	}

	etcdErr, ok := err.(*etcd.EtcdError)
	if ok && etcdErr.ErrorCode == EcodeKeyNotFound {
		_, err := ec.Client.CreateDir(path, ttl)
		if err != nil {
			metafora.Debugf("Error trying to create directory. path:[%s] error:[ %v ]", path, err)
		}
		host, _ := os.Hostname()

		metadata := struct {
			Host        string `json:"host"`
			CreatedTime string `json:"created"`
			ownerValue
		}{
			Host:        host,
			CreatedTime: time.Now().String(),
			ownerValue:  ownerValue{Node: ec.NodeID},
		}
		metadataB, _ := json.Marshal(metadata)
		metadataStr := string(metadataB)
		ec.Client.Create(pathMarker, metadataStr, ttl)
	}
}

// nodeRefresher is in charge of keeping the node entry in etcd alive. If it's
// unable to communicate with etcd it must shutdown the coordinator.
//
// watch retries on errors and taskmgr calls Lost(task) on tasks it can't
// refresh, so it's up to nodeRefresher to cause the coordinator to close if
// it's unable to communicate with etcd.
func (ec *EtcdCoordinator) nodeRefresher() {
	ttl := ec.nodePathTTL >> 1 // have some leeway before ttl expires
	if ttl < 1 {
		ttl = 1
	}
	for {
		// Deadline for refreshes to finish by or the coordinator closes.
		deadline := time.Now().Add(time.Duration(ec.nodePathTTL) * time.Second)
		select {
		case <-ec.stop:
			return
		case <-time.After(time.Duration(ttl) * time.Second):
			if err := ec.refreshBy(deadline); err != nil {
				// We're in a bad state; shut everything down
				metafora.Errorf("Unable to refresh node key before deadline %s. Last error: %v", deadline, err)
				ec.Close()
			}
		}
	}
}

// refreshBy retries refreshing the node key until the deadline is reached.
func (ec *EtcdCoordinator) refreshBy(deadline time.Time) (err error) {
	for time.Now().Before(deadline) {
		// Make sure we shouldn't exit
		select {
		case <-ec.stop:
			return err
		default:
		}

		_, err = ec.Client.UpdateDir(ec.nodePath, ec.nodePathTTL)
		if err == nil {
			// It worked!
			return nil
		}
		metafora.Warnf("Unexpected error updating node key: %v", err)
		transport.CloseIdleConnections()   // paranoia; let's get fresh connections on errors.
		time.Sleep(500 * time.Millisecond) // rate limit retries a bit
	}
	// Didn't get a successful response before deadline, exit with error
	return err
}

// Watch will do a blocking etcd watch on taskPath until a claimable task is
// found or Close() is called.
//
// Watch will return ("", nil) if the coordinator is closed.
func (ec *EtcdCoordinator) Watch() (taskID string, err error) {
	if ec.closed() {
		// already closed, don't restart watch
		return "", nil
	}

	const sorted = true
	const recursive = true

startWatch:
	for {
		// Get existing tasks
		resp, err := ec.Client.Get(ec.taskPath, sorted, recursive)
		if err != nil {
			metafora.Errorf("%s Error getting the existing tasks: %v", ec.taskPath, err)
			return "", err
		}

		// Start watching at the index the Get retrieved since we've retrieved all
		// tasks up to that point.
		index := resp.EtcdIndex

		// Act like existing keys are newly created
		for _, node := range resp.Node.Nodes {
			if node.ModifiedIndex > index {
				// Record the max modified index to keep Watch from picking up redundant events
				index = node.ModifiedIndex
			}
			if task, ok := ec.parseTask(&etcd.Response{Action: "create", Node: node}); ok {
				return task, nil
			}
		}

		// Start blocking watch
		for {
			resp, err := ec.watch(ec.taskPath, index)
			if err != nil {
				if err == restartWatchError {
					continue startWatch
				}
				if err == etcd.ErrWatchStoppedByUser {
					return "", nil
				}
			}

			// Found a claimable task! Return it.
			if task, ok := ec.parseTask(resp); ok {
				return task, nil
			}

			// Task wasn't claimable, start next watch from where the last watch ended
			index = resp.EtcdIndex
		}
	}
}

func (ec *EtcdCoordinator) parseTask(resp *etcd.Response) (task string, ok bool) {
	// Sanity check / test path invariant
	if !strings.HasPrefix(resp.Node.Key, ec.taskPath) {
		metafora.Errorf("Received task from outside task path: %s", resp.Node.Key)
		return "", false
	}

	key := strings.Trim(resp.Node.Key, "/") // strip leading and trailing /s
	parts := strings.Split(key, "/")

	// Pickup new tasks
	if newActions[resp.Action] && len(parts) == 3 && resp.Node.Dir {
		// Make sure it's not already claimed before returning it
		for _, n := range resp.Node.Nodes {
			if strings.HasSuffix(n.Key, OwnerMarker) {
				metafora.Debugf("Ignoring task as it's already claimed: %s", parts[2])
				return "", false
			}
		}
		metafora.Debugf("Received new task: %s", parts[2])
		return parts[2], true
	}

	// If a claim key is removed, try to claim the task
	if releaseActions[resp.Action] && len(parts) == 4 && parts[3] == OwnerMarker {
		metafora.Debugf("Received released task: %s", parts[2])
		return parts[2], true
	}

	// Ignore any other key events (_metafora keys, task deletion, etc.)
	return "", false
}

// Claim is called by the Consumer when a Balancer has determined that a task
// ID can be claimed. Claim returns false if another consumer has already
// claimed the ID.
func (ec *EtcdCoordinator) Claim(taskID string) bool {
	return ec.taskManager.add(taskID)
}

// Release deletes the claim file.
func (ec *EtcdCoordinator) Release(taskID string) {
	const done = false
	ec.taskManager.remove(taskID, done)
}

// Done deletes the task.
func (ec *EtcdCoordinator) Done(taskID string) {
	const done = true
	ec.taskManager.remove(taskID, done)
}

// Command blocks until a command for this node is received from the broker
// by the coordinator.
func (ec *EtcdCoordinator) Command() (metafora.Command, error) {
	if ec.closed() {
		// already closed, don't restart watch
		return nil, nil
	}

	const sorted = true
	const recursive = true
startWatch:
	for {
		// Get existing commands
		resp, err := ec.Client.Get(ec.commandPath, sorted, recursive)
		if err != nil {
			metafora.Errorf("%s Error getting the existing commands: %v", ec.commandPath, err)
			return nil, err
		}

		// Start watching at the index the Get retrieved since we've retrieved all
		// tasks up to that point.
		index := resp.EtcdIndex

		// Act like existing keys are newly created
		for _, node := range resp.Node.Nodes {
			if node.ModifiedIndex > index {
				// Record the max modified index to keep Watch from picking up redundant events
				index = node.ModifiedIndex
			}
			if cmd := ec.parseCommand(&etcd.Response{Action: "create", Node: node}); cmd != nil {
				return cmd, nil
			}
		}

		for {
			resp, err := ec.watch(ec.commandPath, index)
			if err != nil {
				if err == restartWatchError {
					continue startWatch
				}
				if err == etcd.ErrWatchStoppedByUser {
					return nil, nil
				}
			}

			if cmd := ec.parseCommand(resp); cmd != nil {
				return cmd, nil
			}

			index = resp.EtcdIndex
		}
	}
}

func (ec *EtcdCoordinator) parseCommand(resp *etcd.Response) metafora.Command {
	if strings.HasSuffix(resp.Node.Key, MetadataKey) {
		// Skip metadata marker
		return nil
	}

	const recurse = false
	if _, err := ec.Client.Delete(resp.Node.Key, recurse); err != nil {
		metafora.Errorf("Error deleting handled command %s: %v", resp.Node.Key, err)
	}

	cmd, err := metafora.UnmarshalCommand([]byte(resp.Node.Value))
	if err != nil {
		metafora.Errorf("Invalid command %s: %v", resp.Node.Key, err)
		return nil
	}
	return cmd
}

// Close stops the coordinator and causes blocking Watch and Command methods to
// return zero values.
func (ec *EtcdCoordinator) Close() {
	// Gracefully handle multiple close calls mostly to ease testing. This block
	// isn't threadsafe, so you shouldn't try to call Close() concurrently.
	select {
	case <-ec.stop:
		return
	default:
	}

	close(ec.stop)

	ec.taskManager.stop()

	// Finally remove the node entry
	const recursive = true
	_, err := ec.Client.Delete(ec.nodePath, recursive)
	if err != nil {
		if eerr, ok := err.(*etcd.EtcdError); ok {
			if eerr.ErrorCode == EcodeKeyNotFound {
				// The node's TTL was up before we were able to delete it or there was
				// another problem that's already being handled.
				// The first is unlikely, the latter is already being handled, so
				// there's nothing to do here.
				return
			}
		}
		// All other errors are unexpected
		metafora.Errorf("Error deleting node path %s: %v", ec.nodePath, err)
	}
}

// watch will return either an etcd Response or an error. Two errors returned
// by this method should be treated specially:
//
//   1. etcd.ErrWatchStoppedByUser - the coordinator has closed, exit
//                                   accordingly
//
//   2. restartWatchError - the specified index is too old, try again with a
//                          newer index
func (ec *EtcdCoordinator) watch(path string, index uint64) (*etcd.Response, error) {
	const recursive = true
	for {
		// Start the blocking watch after the last response's index.
		rawResp, err := ec.Client.RawWatch(path, index+1, recursive, nil, ec.stop)
		if err != nil {
			if err == etcd.ErrWatchStoppedByUser {
				// This isn't actually an error, the stop chan was closed. Time to stop!
				return nil, err
			}

			// Other RawWatch errors should be retried forever. If the node refresher
			// also fails to communicate with etcd it will close the coordinator,
			// closing ec.stop in the process which will cause this function to with
			// ErrWatchStoppedByUser.
			metafora.Errorf("%s Retrying after unexpected watch error: %v", path, err)
			transport.CloseIdleConnections() // paranoia; let's get fresh connections on errors.
			continue
		}

		if len(rawResp.Body) == 0 {
			// This is a bug in Go's HTTP + go-etcd + etcd which causes the
			// connection to timeout perdiocally and need to be restarted *after*
			// closing idle connections.
			transport.CloseIdleConnections()
			continue
		}

		resp, err := rawResp.Unmarshal()
		if err != nil {
			if ee, ok := err.(*etcd.EtcdError); ok {
				if ee.ErrorCode == EcodeExpiredIndex {
					metafora.Debugf("%s Too many events have happened since index was updated. Restarting watch.", ec.taskPath)
					// We need to retrieve all existing tasks to update our index
					// without potentially missing some events.
					return nil, restartWatchError
				}
			}
			metafora.Errorf("%s Unexpected error unmarshalling etcd response: %+v", ec.taskPath, err)
			return nil, err
		}
		return resp, nil
	}
}
