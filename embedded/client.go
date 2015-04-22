package embedded

import "github.com/lytics/metafora"

func NewEmbeddedClient(taskchan chan<- metafora.Task, cmdchan chan<- *NodeCommand) metafora.Client {
	return &EmbeddedClient{taskchan, cmdchan}
}

type EmbeddedClient struct {
	taskchan chan<- metafora.Task
	cmdchan  chan<- *NodeCommand
}

func (ec *EmbeddedClient) SubmitTask(id string, props map[string]string) error {
	ec.taskchan <- NewTask(id, props)
	return nil
}

func (ec *EmbeddedClient) SubmitCommand(nodeid string, command metafora.Command) error {
	ec.cmdchan <- &NodeCommand{command, nodeid}
	return nil
}
