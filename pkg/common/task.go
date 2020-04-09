//go:generate mockgen -destination=./mocks/mock_task.go -package=mocks . TaskHandler,TaskPayloadHandler,TaskProcessEventHandler,TaskQueueEventHandler
// Package common provides primitives for task registration and marshalling of task request data
package common

import (
	"github.com/mnikita/task-queue/pkg/log"
)

const (
	Error = iota
	Success
	Heartbeat
	Result
)

//TaskHandler handles task requests. Final task implements TaskHandle interface
type TaskHandler interface {
	SetTaskProcessEventHandler(eventHandler TaskProcessEventHandler)
	SetTask(task *Task)
	Handle() error
}

//TaskPayloadHandler handles consumer task payload.
//TaskPayloadHandler implementation dispatches consumer requests to worker queue
type TaskPayloadHandler interface {
	HandlePayload(task *Task)
}

//TaskProcessEventHandler handles events arising from final task processing
type TaskProcessEventHandler interface {
	OnTaskSuccess(task *Task)
	OnTaskHeartbeat(task *Task)
	OnTaskError(task *Task, err error)

	OnTaskResult(task *Task, a ...interface{})
}

//TaskQueueEventHandler handles task queue events
type TaskQueueEventHandler interface {
	OnTaskQueued(task *Task)
	OnTaskAcceptTimeout(task *Task)
}

var eventTypes = []string{"Error", "Success", "Heartbeat", "Result"}

//TaskProcessEvent struct contains task process event data
type TaskProcessEvent struct {
	EventId int
	Task    *Task
	Err     error
	Result  []interface{}
}

//Task struct contains task requests data
type Task struct {
	Id      uint64
	Name    string
	Payload []byte
}

//TaskHandlerFunc is helper class for creating short task implementation containing one processing function
type TaskHandlerFunc func(task *Task, eventHandler TaskProcessEventHandler) error

//BaseTaskHandler is base primitive for final tasks implementations.
//It is a default implementation of TaskHandler interface
type BaseTaskHandler struct {
	eventHandler TaskProcessEventHandler `json:"-"`
	task         *Task                   `json:"-"`
	handler      TaskHandlerFunc         `json:"-"`
}

//TaskThreadError is default error thrown during task processing
type TaskThreadError struct {
	Err  error
	Task *Task
}

//TaskConstructor creates TaskHandler instances
type TaskConstructor func() TaskHandler

func NewBaseTaskHandler(handler TaskHandlerFunc) *BaseTaskHandler {
	return &BaseTaskHandler{handler: handler}
}

func (b *BaseTaskHandler) SetTask(task *Task) {
	b.task = task
}

func (b *BaseTaskHandler) SetTaskProcessEventHandler(eventHandler TaskProcessEventHandler) {
	b.eventHandler = eventHandler
}

func (b *BaseTaskHandler) Handle() error {
	return b.handler(b.task, b.eventHandler)
}

func NewTaskThreadError(task *Task, err error) *TaskThreadError {
	return &TaskThreadError{Task: task, Err: log.TaskThreadError(task.Name, err)}
}

func (e *TaskProcessEvent) GetEventType() string {
	return eventTypes[e.EventId]
}

func (err *TaskThreadError) Error() string {
	return err.Err.Error()
}
