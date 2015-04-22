package m_etcd

import (
	"fmt"
	"testing"

	"github.com/coreos/go-etcd/etcd"
	"github.com/lytics/metafora"
)

type ctx struct{}

func (ctx) Lost(string)                                   {}
func (ctx) Log(metafora.LogLevel, string, ...interface{}) {}

type taskTest struct {
	Resp  *etcd.Response
	Resp2 *etcd.Response // response to second GET
	Task  metafora.Task
	Ok    bool
}

func (t taskTest) Get(key string, _ bool, _ bool) (*etcd.Response, error) {
	if t.Resp2 == nil {
		return nil, fmt.Errorf("no response")
	}
	return t.Resp2, nil
}

func (t taskTest) propsEqual(other map[string]string) bool {
	me := t.Task.Props()
	for k, v := range other {
		if me[k] != v {
			return false
		}
	}
	for k, v := range me {
		if other[k] != v {
			return false
		}
	}
	return true
}

func TestParseTask(t *testing.T) {
	path := "/namespace/tasks"

	tests := []taskTest{
		// bad
		{Resp: &etcd.Response{Action: actionCAD, Node: &etcd.Node{Key: "/namespace/tasks/1", Dir: true}}},
		{Resp: &etcd.Response{Action: actionCreated, Node: &etcd.Node{Key: "/namespace/tasks/2/a", Dir: true}}},
		{Resp: &etcd.Response{Action: actionCAD, Node: &etcd.Node{Key: "/namespace/tasks/3", Dir: false}}},
		{
			Resp: &etcd.Response{Action: actionCAD, Node: &etcd.Node{Key: "/namespace/tasks/3.1/owner"}},
			Resp2: &etcd.Response{Node: &etcd.Node{Nodes: etcd.Nodes{
				&etcd.Node{Key: "/namespace/tasks/3.1/props", Value: `invalid json`},
			}}},
			Ok: false,
		},

		// good
		{
			Resp: &etcd.Response{Action: actionCreated, Node: &etcd.Node{Key: "/namespace/tasks/4", Dir: true}},
			Task: &task{id: "4"},
			Ok:   true,
		},
		{
			Resp: &etcd.Response{Action: actionSet, Node: &etcd.Node{Key: "/namespace/tasks/5", Dir: true}},
			Task: &task{id: "5"},
			Ok:   true,
		},
		{
			Resp: &etcd.Response{Action: actionCAD, Node: &etcd.Node{Key: "/namespace/tasks/6/owner"}},
			Resp2: &etcd.Response{Node: &etcd.Node{Nodes: etcd.Nodes{
				&etcd.Node{Key: "/namespace/tasks/6/ignoreme", Value: "-"},
				&etcd.Node{Key: "/namespace/tasks/6/props", Value: `{"k":"v"}`},
			}}},
			Task: &task{id: "6", props: map[string]string{"k": "v"}},
			Ok:   true,
		},
		{
			Resp: &etcd.Response{Action: actionDelete, Node: &etcd.Node{Key: "/namespace/tasks/7/owner"}},
			Resp2: &etcd.Response{Node: &etcd.Node{Nodes: etcd.Nodes{
				&etcd.Node{Key: "/namespace/tasks/6/props", Value: `{"x":"1", "y": "2"}`},
			}}},
			Task: &task{id: "7", props: map[string]string{"x": "1", "y": "2"}},
			Ok:   true,
		},
	}

	for _, test := range tests {
		task, ok := parseTask(test, test.Resp, path)
		switch {
		case test.Task == nil && task != nil:
			t.Errorf("Test %s:%v failed: expected nil but received: %v", test.Resp.Action, test.Resp.Node.Key, task)
		case test.Task != nil && task == nil:
			t.Errorf("Test %s:%v failed: received nil but expected: %v", test.Resp.Action, test.Resp.Node.Key, test.Task)
		case (task != nil && test.Task != nil) && task.ID() != test.Task.ID():
			t.Errorf("Test %s:%v failed: %s != %s", test.Resp.Action, test.Resp.Node.Key, task.ID(), test.Task.ID())
		case (task != nil && test.Task != nil) && !test.propsEqual(task.Props()):
			t.Errorf("Test %s:%v failed: %#v != %#v", test.Resp.Action, test.Resp.Node.Key, task.Props(), test.Task.Props())
		case ok != test.Ok:
			t.Errorf("Test %s:%v failed: %v != %v", test.Resp.Action, test.Resp.Node.Key, ok, test.Ok)
		default:
			t.Logf("Test %s:%v OK", test.Resp.Action, test.Resp.Node.Key)
		}
	}
}
