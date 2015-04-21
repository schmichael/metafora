package embedded

import "github.com/lytics/metafora"

type NodeCommand struct {
	Cmd    metafora.Command
	NodeId string
}

// Returns a connected client/coordinator pair for embedded/testing use
func NewEmbeddedPair(nodeid string) (metafora.Coordinator, metafora.Client) {
	taskchan := make(chan metafora.Task)
	cmdchan := make(chan *NodeCommand)

	coord := NewEmbeddedCoordinator(nodeid, taskchan, cmdchan)
	client := NewEmbeddedClient(taskchan, cmdchan)

	return coord, client
}
