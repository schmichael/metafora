package m_etcd

import (
	"encoding/json"
	"path"
	"sync"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
	"github.com/lytics/metafora/statemachine"
)

const (
	commandPath = "commands"

	// cmdTTL is the TTL in seconds set on commands so that commands sent to
	// terminating work isn't orphaned in etcd forever.
	cmdTTL = 7 * 24 * 60 * 60 // 1 week in seconds
)

type cmdr struct {
	cli  *etcd.Client
	path string
}

func NewCommander(c *etcd.Client, namespace string) statemachine.Commander {
	if namespace[0] != '/' {
		namespace = "/" + namespace
	}
	return &cmdr{path: path.Join(namespace, commandPath), cli: c}
}

// Send command to a task. Overwrites existing commands.
func (c *cmdr) Send(taskID string, m statemachine.Message) error {
	buf, err := json.Marshal(m)
	if err != nil {
		return err
	}

	_, err = c.cli.Set(path.Join(c.path, taskID), string(buf), cmdTTL)
	return err
}

type cmdrListener struct {
	cli  *etcd.Client
	path string

	commands chan statemachine.Message

	mu   *sync.Mutex
	stop chan bool
}

func NewCommandListener(c *etcd.Client, namespace string, taskID string) statemachine.CommandListener {
	if namespace[0] != '/' {
		namespace = "/" + namespace
	}
	cl := &cmdrListener{
		path:     path.Join(namespace, commandPath, taskID),
		cli:      c,
		commands: make(chan statemachine.Message),
		mu:       &sync.Mutex{},
		stop:     make(chan bool),
	}
	go cl.watcher()
	return cl
}

func (c *cmdrListener) Receive() <-chan statemachine.Message { return c.commands }
func (c *cmdrListener) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.stop:
	default:
		close(c.stop)
	}
}

func (c *cmdrListener) sendErr(err error) {
	select {
	case c.commands <- statemachine.Message{Code: statemachine.Error, Err: err}:
	case <-c.stop:
	}
}

func (c *cmdrListener) sendMsg(resp *etcd.Response) (index uint64, ok bool) {
	// Only handle new commands
	if !newActions[resp.Action] {
		return resp.Node.ModifiedIndex, true
	}

	// Remove command so it's not processed twice
	cadresp, err := c.cli.CompareAndDelete(resp.Node.Key, resp.Node.Value, 0)
	if err != nil {
		if ee, ok := err.(*etcd.EtcdError); ok && ee.ErrorCode == EcodeCompareFailed {
			metafora.Infof("Received successive commands; attempting to retrieve the latest: %v", err)
			return resp.Node.ModifiedIndex, true
		}
		metafora.Errorf("Error deleting command %s: %s - sending error to stateful handler: %v", c.path, resp.Node.Value, err)
		c.sendErr(err)
		return 0, false
	}

	msg := statemachine.Message{}
	if err := json.Unmarshal([]byte(resp.Node.Value), &msg); err != nil {
		metafora.Errorf("Error unmarshalling command from %s - sending error to stateful handler: %v", c.path, err)
		c.sendErr(err)
		return 0, false
	}

	select {
	case c.commands <- msg:
		return cadresp.Node.ModifiedIndex, true
	case <-c.stop:
		return 0, false
	}
}

func (c *cmdrListener) watcher() {
	var index uint64
	var ok bool
startWatch:
	const recursive = false
	const sort = false
	resp, err := c.cli.Get(c.path, recursive, sort)
	if err == nil {
		if ee, ok := err.(*etcd.EtcdError); ok && ee.ErrorCode == EcodeKeyNotFound {
			// No command found; this is normal. Grab index and skip to watching
			index = ee.Index
			goto watchLoop
		}
		metafora.Errorf("Error GETting %s - sending error to stateful handler: %v", c.path, err)
		c.sendErr(err)
		return
	}

	if index, ok = c.sendMsg(resp); !ok {
		return
	}

watchLoop:
	for {
		rr, err := c.cli.RawWatch(c.path, index, recursive, nil, c.stop)
		if err != nil {
			if err == etcd.ErrWatchStoppedByUser {
				return
			}
			metafora.Errorf("Error watching %s - sending error to stateful handler: %v", c.path, err)
			c.sendErr(err)
			return
		}

		if len(rr.Body) == 0 {
			// This is a bug in Go's HTTP + go-etcd + etcd which causes the
			// connection to timeout perdiocally and need to be restarted *after*
			// closing idle connections.
			transport.CloseIdleConnections()
			continue watchLoop
		}

		resp, err := rr.Unmarshal()
		if err != nil {
			if ee, ok := err.(*etcd.EtcdError); ok {
				if ee.ErrorCode == EcodeExpiredIndex {
					goto startWatch
				}
			}
			metafora.Errorf("Error watching %s - sending error to stateful handler: %v", c.path, err)
			c.sendErr(err)
			return
		}

		if index, ok = c.sendMsg(resp); !ok {
			return
		}

		// Update the watch index
		index = resp.Node.ModifiedIndex
	}
}
