package metafora

type Client interface {
	// SubmitTask submits a task to the broker for Metafora consumers to claim.
	// The ID must be unique. Properties may be nil.
	SubmitTask(id string, props map[string]string) error

	// SubmitCommand submits a command to a particular node.
	SubmitCommand(node string, command Command) error
}
