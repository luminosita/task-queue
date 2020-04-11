//go:generate mockgen -destination=./mocks/mock_connector.go -package=mocks . Handler
package connector

import (
	"github.com/google/wire"
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/mnikita/task-queue/pkg/log"
	"github.com/mnikita/task-queue/pkg/util"
	"time"
)

var WireSet = wire.NewSet(NewConnector, NewConfiguration,
	wire.Bind(new(Handler), new(*Connector)))

type Handler interface {
	Init() error
	Close() error

	SetTaskEventChannel(eventChannel chan<- *common.TaskProcessEvent)
	SetTaskQueueChannel(taskQueueChannel chan<- *common.Task)
	SetEventHandler(eventHandler common.TaskQueueEventHandler)
}

type Connector struct {
	taskEventChannel chan<- *common.TaskProcessEvent

	taskQueueChannel chan<- *common.Task

	eventHandler common.TaskQueueEventHandler

	*Configuration
}

type Configuration struct {
	WaitToAcceptConsumerTask time.Duration
	WaitToAcceptEvent        time.Duration
}

func (c *Connector) sendProcessEvent(event *common.TaskProcessEvent) {
	//send events asynchronously to avoid blocking execution thread for a task
	//timeout to prevent thread leaks
	go func() {
		select {
		case c.taskEventChannel <- event:
		case <-time.After(c.WaitToAcceptEvent):
			log.Logger().TaskProcessEventTimeout(event.GetEventType(), event.Task.Name, c.WaitToAcceptEvent)
		}
	}()
}

func NewConnector(config *Configuration) *Connector {
	c := &Connector{Configuration: config}

	return c
}

func NewConfiguration() *Configuration {
	return &Configuration{
		WaitToAcceptConsumerTask: time.Second * 10,
		WaitToAcceptEvent:        time.Millisecond * 100,
	}
}

func (c *Connector) Init() error {
	return nil
}

func (c *Connector) Close() error {
	return nil
}

func (c *Connector) SetTaskEventChannel(
	eventChannel chan<- *common.TaskProcessEvent) {
	c.taskEventChannel = eventChannel
}

func (c *Connector) SetTaskQueueChannel(taskQueueChannel chan<- *common.Task) {
	c.taskQueueChannel = taskQueueChannel
}

func (c *Connector) SetEventHandler(eventHandler common.TaskQueueEventHandler) {
	c.eventHandler = eventHandler
}

//Handles task payload from consumer
func (c *Connector) HandlePayload(task *common.Task) {
	select {
	case c.taskQueueChannel <- task:
		c.OnTaskQueued(task)
	case <-time.After(c.WaitToAcceptConsumerTask):
		c.OnTaskAcceptTimeout(task)
	}
}

func (c *Connector) OnTaskSuccess(task *common.Task) {
	c.sendProcessEvent(&common.TaskProcessEvent{EventId: common.Success,
		Task: task})
}

func (c *Connector) OnTaskHeartbeat(task *common.Task) {
	c.sendProcessEvent(&common.TaskProcessEvent{EventId: common.Heartbeat,
		Task: task})
}

func (c *Connector) OnTaskError(task *common.Task, err error) {
	c.sendProcessEvent(&common.TaskProcessEvent{EventId: common.Error,
		Task: task,
		Err:  err})
}

func (c *Connector) OnTaskResult(task *common.Task, a ...interface{}) {
	c.sendProcessEvent(&common.TaskProcessEvent{EventId: common.Result,
		Task:   task,
		Result: a})
}

func (c *Connector) OnTaskQueued(task *common.Task) {
	log.Logger().TaskQueued(task.Name)

	if !util.IsNil(c.eventHandler) {
		c.eventHandler.OnTaskQueued(task)
	}
}

func (c *Connector) OnTaskAcceptTimeout(task *common.Task) {
	log.Logger().TaskQueueTimeout(task.Name, c.WaitToAcceptConsumerTask)

	if !util.IsNil(c.eventHandler) {
		c.eventHandler.OnTaskAcceptTimeout(task)
	}
}
