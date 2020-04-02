// Package common provides primitives for task registration and marshalling of task request data
package common

//TaskHandler handles task requests
type TaskHandler interface {
	Handle(callback func(a ...interface{})) error
}

//Task struct contains task requests data
type Task struct {
	Name    string
	Payload []byte
}

//TaskConstructor creates TaskHandler instances
type TaskConstructor func() TaskHandler
