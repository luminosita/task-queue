package connector

import (
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/mnikita/task-queue/pkg/log"
	"time"
)

type Connector struct {
	taskEventChannel chan<- *common.TaskProcessEvent

	taskQueueChannel chan<- *common.Task

	eventHandler common.TaskQueueEventHandler

	config *Configuration
}

type Configuration struct {
	WaitToAcceptConsumerTask time.Duration
	WaitToAcceptEvent        time.Duration
}

func NewConfiguration() *Configuration {
	return &Configuration{
		WaitToAcceptConsumerTask: time.Second * 10,
		WaitToAcceptEvent:        time.Second * 10,
	}
}

func NewConnector(config *Configuration) *Connector {
	if config == nil {
		config = NewConfiguration()
	}

	connector := &Connector{config: config}

	return connector
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
	case <-time.After(c.config.WaitToAcceptConsumerTask):
		c.OnTaskAcceptTimeout(task)
	}
}

func (c *Connector) OnTaskProcessEvent(event *common.TaskProcessEvent) {
	select {
	case c.taskEventChannel <- event:
	case <-time.After(c.config.WaitToAcceptEvent):
		log.Logger().TaskProcessEventTimeout(event.GetEventType(), event.Task.Name, c.config.WaitToAcceptEvent)
	}
}

//TODO: Implement
func (c *Connector) OnResult(a ...interface{}) error {
	panic("implement me")
}

func (c *Connector) OnTaskQueued(task *common.Task) {
	log.Logger().TaskQueued(task.Name)

	if c.eventHandler != nil {
		c.eventHandler.OnTaskQueued(task)
	}
}

func (c *Connector) OnTaskAcceptTimeout(task *common.Task) {
	log.Logger().TaskQueueTimeout(task.Name, c.config.WaitToAcceptConsumerTask)

	if c.eventHandler != nil {
		c.eventHandler.OnTaskAcceptTimeout(task)
	}
}
