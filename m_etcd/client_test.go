package m_etcd

import (
	"encoding/json"
	"testing"

	"github.com/lytics/metafora"
)

// TestSubmitTask tests that client.SubmitTask(...) adds a task to
// the proper path in etcd, and that the same task id cannot be
// submitted more than once.
func TestSubmitTask(t *testing.T) {
	_, eclient := setupEtcd(t)

	mclient := NewClient(namespace, eclient)

	if err := mclient.SubmitTask("testid1", nil); err != nil {
		t.Fatalf("Submit task failed on initial submission, error: %v", err)
	}

	if err := mclient.SubmitTask("testid1", nil); err == nil {
		t.Fatalf("Submit task did not fail, but should of, when using existing tast id")
	}

	if err := mclient.SubmitTask("testid2", map[string]string{"k": "v"}); err != nil {
		t.Fatalf("Submit task (2) failed on initial submission, error: %v", err)
	}
	resp, err := eclient.Get(namespace+"/tasks/testid2/props", false, false)
	if err != nil {
		t.Fatalf("Error retrieving task 2's properties: %v", err)
	}

	var props map[string]string
	err = json.Unmarshal([]byte(resp.Node.Value), &props)
	if err != nil {
		t.Fatalf("Error unmarshalling properties: %v", err)
	}
	if len(props["_submitted"]) == 0 {
		t.Errorf("Missing builtin _submitted key: %#v", props)
	}
	if props["k"] != "v" {
		t.Errorf("Didn't find expected property k=v: k=%q", props["k"])
	}
}

// TestSubmitCommand tests that client.SubmitCommand(...) adds a command
// to the proper node path in etcd, and that it can be read back.
func TestSubmitCommand(t *testing.T) {
	_, eclient := setupEtcd(t)

	mclient := NewClient(namespace, eclient)

	if err := mclient.SubmitCommand(nodeID, metafora.CommandFreeze()); err != nil {
		t.Fatalf("Unable to submit command.   error:%v", err)
	}

	ndir := namespace + "/nodes"
	if res, err := eclient.Get(ndir, false, false); err != nil {
		t.Fatalf("Get on path %v returned error: %v", ndir, err)
	} else if res.Node == nil || res.Node.Nodes == nil {
		t.Fatalf("Get on path %v returned nil for child nodes", ndir)
	} else {
		for i, n := range res.Node.Nodes {
			t.Logf("%v -> %v", i, n)
		}
	}
}
