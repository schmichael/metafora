package m_etcd

import (
	"testing"
	"time"

	"github.com/lytics/metafora"
)

func TestFairBalancer(t *testing.T) {
	coord1, etcdc := setupEtcd(t)
	coord2 := NewEtcdCoordinator("node2", namespace, etcdc).(*EtcdCoordinator)

	cli := NewClient(namespace, etcdc)

	running := make(chan int, 100)
	h := metafora.SimpleHandler(func(task string, stop <-chan bool) bool {
		running <- 1
		metafora.Debugf("Starting %s", task)
		<-stop
		metafora.Debugf("Stopping %s", task)
		return false // never done
	})

	// Create two consumers
	b1 := NewFairBalancer(nodeID, namespace, etcdc)
	con1, err := metafora.NewConsumer(coord1, h, b1)
	if err != nil {
		t.Fatal(err)
	}

	b2 := NewFairBalancer("node2", namespace, etcdc)
	con2, err := metafora.NewConsumer(coord2, h, b2)
	if err != nil {
		t.Fatal(err)
	}

	// Start the first and let it claim a bunch of tasks
	go con1.Run()
	defer con1.Shutdown()
	cli.SubmitTask("t1", nil)
	cli.SubmitTask("t2", nil)
	cli.SubmitTask("t3", nil)
	cli.SubmitTask("t4", nil)
	cli.SubmitTask("t5", nil)
	cli.SubmitTask("t6", nil)

	for i := 0; i < 6; i++ {
		<-running
	}

	if len(con1.Tasks()) != 6 {
		t.Fatalf("con1 should have claimed 6 tasks: %d", len(con1.Tasks()))
	}

	// Start the second consumer and force the 1st to rebalance
	go con2.Run()
	defer con2.Shutdown()

	// Wait for node to startup and register
	time.Sleep(500 * time.Millisecond)

	cli.SubmitCommand(nodeID, metafora.CommandBalance())

	for i := 0; i < 2; i++ {
		select {
		case <-running:
		case <-time.After(10 * time.Second):
		}
	}

	c1Tasks := con1.Tasks()
	c2Tasks := con2.Tasks()
	if len(c1Tasks) != 4 || len(c2Tasks) != 2 {
		t.Fatalf("expected consumers to have 4|2 tasks: %d|%d", len(c1Tasks), len(c2Tasks))
	}

	// Finally make sure that balancing the other node does nothing
	cli.SubmitCommand("node2", metafora.CommandBalance())

	time.Sleep(2 * time.Second)

	c1Tasks2 := con1.Tasks()
	c2Tasks2 := con2.Tasks()
	if len(c1Tasks2) != 4 || len(c2Tasks2) != 2 {
		t.Fatalf("expected consumers to have 4|2 tasks: %d|%d", len(c1Tasks2), len(c2Tasks2))
	}
	for i := 0; i < 4; i++ {
		if c1Tasks[i] != c1Tasks2[i] {
			t.Errorf("task mismatch: %s != %s", c1Tasks[i], c1Tasks2[i])
		}
	}
	for i := 0; i < 2; i++ {
		if c2Tasks[i] != c2Tasks2[i] {
			t.Errorf("task mismatch: %s != %s", c2Tasks[i], c2Tasks2[i])
		}
	}
}

// Fair balancer shouldn't consider a shutting-down node
// See https://github.com/lytics/metafora/issues/92
func TestFairBalancerShutdown(t *testing.T) {
	coord1, etcdc := setupEtcd(t)
	coord2 := NewEtcdCoordinator("node2", namespace, etcdc).(*EtcdCoordinator)

	cli := NewClient(namespace, etcdc)

	running := make(chan int, 100)

	// This handler always returns immediately
	h1 := metafora.SimpleHandler(func(task string, stop <-chan bool) bool {
		running <- 1
		metafora.Debugf("H1 Starting %s", task)
		<-stop
		metafora.Debugf("H1 Stopping %s", task)
		return false // never done
	})

	// Block forever on a single task
	stop2 := make(chan struct{})
	stopr := make(chan chan struct{}, 1)
	stopr <- stop2
	h2 := metafora.SimpleHandler(func(task string, stop <-chan bool) bool {
		running <- 1
		metafora.Debugf("H2 Starting %s", task)
		blockchan, ok := <-stopr
		if ok {
			<-blockchan
		}
		<-stop
		metafora.Debugf("H2 Stopping %s", task)
		return false // never done
	})

	// Create two consumers
	b1 := NewFairBalancer(nodeID, namespace, etcdc)
	con1, err := metafora.NewConsumer(coord1, h1, b1)
	if err != nil {
		t.Fatal(err)
	}

	b2 := NewFairBalancer("node2", namespace, etcdc)
	con2, err := metafora.NewConsumer(coord2, h2, b2)
	if err != nil {
		t.Fatal(err)
	}

	// Start the first and let it claim a bunch of tasks
	go con1.Run()
	defer con1.Shutdown()
	cli.SubmitTask("t1", nil)
	cli.SubmitTask("t2", nil)
	cli.SubmitTask("t3", nil)
	cli.SubmitTask("t4", nil)
	cli.SubmitTask("t5", nil)
	cli.SubmitTask("t6", nil)

	for i := 0; i < 6; i++ {
		<-running
	}

	if len(con1.Tasks()) != 6 {
		t.Fatalf("con1 should have claimed 6 tasks: %d", len(con1.Tasks()))
	}

	// Start the second consumer and force the 1st to rebalance
	go con2.Run()

	close(stopr)

	// Wait for node to startup and register
	time.Sleep(500 * time.Millisecond)

	cli.SubmitCommand(nodeID, metafora.CommandBalance())

	for i := 0; i < 2; i++ {
		select {
		case <-running:
		case <-time.After(2 * time.Second):
			t.Fatalf("Should have started a task within 2s")
		}
	}

	c1Tasks := con1.Tasks()
	c2Tasks := con2.Tasks()
	if len(c1Tasks) != 4 || len(c2Tasks) != 2 {
		t.Fatalf("expected consumers to have 4|2 tasks: %d|%d", len(c1Tasks), len(c2Tasks))
	}

	// Make sure that balancing the other node does nothing
	cli.SubmitCommand("node2", metafora.CommandBalance())

	time.Sleep(1500 * time.Millisecond)

	c1Tasks2 := con1.Tasks()
	c2Tasks2 := con2.Tasks()
	if len(c1Tasks2) != 4 || len(c2Tasks2) != 2 {
		t.Fatalf("expected consumers to have 4|2 tasks: %d|%d", len(c1Tasks2), len(c2Tasks2))
	}
	for i := 0; i < 4; i++ {
		if c1Tasks[i] != c1Tasks2[i] {
			t.Errorf("task mismatch: %s != %s", c1Tasks[i], c1Tasks2[i])
		}
	}
	for i := 0; i < 2; i++ {
		if c2Tasks[i] != c2Tasks2[i] {
			t.Errorf("task mismatch: %s != %s", c2Tasks[i], c2Tasks2[i])
		}
	}

	// Second consumer should block on a single task forever
	// Rebalancing the first node should then cause it to pickup all but
	// one task
	c2stop := make(chan struct{})
	go func() {
		con2.Shutdown()
		close(c2stop)
	}()

	time.Sleep(500 * time.Millisecond)

	cli.SubmitCommand(nodeID, metafora.CommandBalance())

	select {
	case <-running:
	case <-time.After(2 * time.Second):
		t.Fatalf("Should have started a task within 2s")
	}

	c1Tasks3 := con1.Tasks()
	c2Tasks3 := con2.Tasks()
	if len(c1Tasks3) != 5 || len(c2Tasks3) != 1 {
		t.Fatalf("Expected consumers to have 5|1 tasks: %d|%d", len(c1Tasks3), len(c2Tasks3))
	}

	// Now stop blocking task, rebalance and make sure the first node picked up the remaining
	close(stop2)

	time.Sleep(500 * time.Millisecond)
	// Consumer 2 should stop now
	<-c2stop

	cli.SubmitCommand(nodeID, metafora.CommandBalance())

	time.Sleep(1500 * time.Millisecond)

	// con2 is out of the picture. con1 has all the tasks.
	c1Tasks4 := con1.Tasks()
	c2Tasks4 := con2.Tasks()
	if len(c1Tasks4) != 6 || len(c2Tasks4) != 0 {
		t.Fatalf("Expected consumers to have 6|0 tasks: %d|%d", len(c1Tasks4), len(c2Tasks4))
	}
}
