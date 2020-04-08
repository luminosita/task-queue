// Package common provides primitives for task registration and marshalling of task request data
package common

import (
	"github.com/mnikita/task-queue/pkg/log"
)

const (
	Error = iota
	Success
	Heartbeat
)

var eventTypes = []string{"Error", "Success", "Heartbeat"}

type TaskProcessEvent struct {
	EventId int
	Task    *Task
	Err     error
}

//Task struct contains task requests data
type Task struct {
	Id      uint64
	Name    string
	Payload []byte
}

type TaskThreadError struct {
	ThreadId int
	Err      error
	Task     *Task
}

//TaskHandler handles task requests
type TaskHandler interface {
	SetTaskProcessEventHandler(eventHandler TaskProcessEventHandler)
	Handle() error
}

func (e *TaskProcessEvent) GetEventType() string {
	return eventTypes[e.EventId]
}

func (err *TaskThreadError) Error() string {
	return err.Err.Error()
}

func NewTaskThreadError(task *Task, threadId int, err error) *TaskThreadError {
	return &TaskThreadError{ThreadId: threadId, Task: task, Err: log.TaskThreadError(task.Name, threadId, err)}
}

//TaskConstructor creates TaskHandler instances
type TaskConstructor func() TaskHandler

//TaskPayloadHandler handles consumer task payload
type TaskPayloadHandler interface {
	HandlePayload(task *Task)
}

type TaskProcessEventHandler interface {
	OnTaskProcessEvent(event *TaskProcessEvent)

	OnResult(a ...interface{}) error
}

type TaskQueueEventHandler interface {
	OnTaskQueued(task *Task)
	OnTaskAcceptTimeout(task *Task)
}
