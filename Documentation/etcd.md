## etcd integration

Metafora contains an [etcd](https://github.com/coreos/etcd) implementation of
the core
[`Coordinator`](https://godoc.org/github.com/lytics/metafora#Coordinator) and
[`Client`](http://godoc.org/github.com/lytics/metafora#Client) interfaces, so
that implementing Metafora with etcd in your own work system is quick and easy.

##### etcd layout

```
/
└── <namespace>
    ├── nodes
    │   └── <node_id>          Ephemeral
    │       └── commands  
    │           └── <command>  JSON value
    └── tasks
        └── <task_id>
            ├── props          JSON value
            └── owner          Ephemeral, JSON value
```

##### Tasks

Metafora clients submit tasks by making either: 

* an empty directory in `/<namespace>/tasks/`
* a new directory containing a single file `/<namespace>/tasks/props`
  * `props` may have a JSON encoded value of string keys and values

Metafora nodes claim tasks by watching the `tasks` directory and -- if
`Balancer.CanClaim` returns `true` -- tries to create the
`/<namespace>/tasks/<tasks_id>/owner` file with the contents set to the nodes
name and a short TTL. The node must touch the file before the TTL expires
otherwise another node will claim the task and begin working on it.

The JSON format is:

```json
{"node": "<node ID>"}
```

Note that Metafora does not handle task parameters or configuration.

##### Commands

Metafora clients send commands by making a file inside
`/<namespace>/nodes/<node_id>/commands/` with any name (preferably using a time-ordered
UUID).

Metafora nodes watch their own node's `commands` directory for new files. The
contents of the files are a command to be executed. Only one command will be
executed at a time, and pending commands are lost on node shutdown.

```json
{"command": "<command name>", "parameters": {}}
```

Where parameters is an arbitrary JSON Object.

### Useful links for managing etcd

[The etcd API](https://coreos.com/docs/distributed-configuration/etcd-api/)

[etcd cli tool](https://github.com/coreos/etcdctl)

