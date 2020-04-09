//go:generate mockgen -destination=./mocks/mock_consumer.go -package=mocks . EventHandler,Handler
package consumer

import (
	"encoding/json"
	"errors"
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/mnikita/task-queue/pkg/connector"
	"github.com/mnikita/task-queue/pkg/log"
	"github.com/mnikita/task-queue/pkg/util"
	"time"
)

type EventHandler interface {
	OnStartConsume()
	OnEndConsume()

	OnReserveTimeout()
	OnHeartbeat()
}

type Handler interface {
	Reserve(timeout time.Duration) (id uint64, body []byte, err error)
	Release(id uint64, pri uint32, delay time.Duration) error
	Delete(id uint64) error
	Bury(id uint64, pri uint32) error
	Touch(id uint64) error

	Close() error
}

//Consumer stores configuration for consumer activation
type Consumer struct {
	handler Handler

	eventHandler EventHandler

	taskPayloadHandler common.TaskPayloadHandler

	config *Configuration

	taskEventChannel chan *common.TaskProcessEvent
	quitChannel      chan bool
}

//Configuration stores initialization data for worker server
type Configuration struct {
	//Waiting time for consumer reserve
	WaitForConsumerReserve time.Duration

	//Waiting time for quit signal timeout
	Heartbeat time.Duration

	ReleasePriority uint32
	ReleaseDelay    time.Duration
	BuryPriority    uint32
}

//hard coded to avoid dependency on go-beanstalkd library only for one constant
var ErrTimeout = errors.New("timeout")

const (
	//Channel size to allocate. It is important for task implementation to send event
	//asynchronously to avoid blocking the execution thread
	TaskEventChannelSize = 1
)

//HandlePayload unmarshal payload data into Task instance to invoke given TaskPayloadHandler
func (con *Consumer) handlePayload(id uint64, body []byte) error {
	if body == nil {
		return log.EmptyReserveTaskPayloadError(id)
	}

	task := &common.Task{Id: id}

	err := json.Unmarshal(body, task)

	if err != nil {
		return log.InvalidReserveTaskPayloadError(id, err)
	}

	con.taskPayloadHandler.HandlePayload(task)

	return nil
}

func (con *Consumer) handleTaskEvent(taskProcessEvent *common.TaskProcessEvent) {
	var err error

	log.Logger().TaskProcessEvent(taskProcessEvent.GetEventType(), taskProcessEvent.Task.Name)

	switch taskProcessEvent.EventId {
	case common.Error:
		err = con.Bury(taskProcessEvent.Task.Id, con.config.BuryPriority)
	case common.Success:
		err = con.Delete(taskProcessEvent.Task.Id)
	case common.Heartbeat:
		err = con.Touch(taskProcessEvent.Task.Id)
	case common.Result:
		//ignoring return result
	}

	if err != nil {
		log.Logger().Error(err)
	}
}

func (con *Consumer) handleConsume() {
	con.OnStartConsume()
	defer con.OnEndConsume()

	for {
		id, body, err := con.Reserve(con.config.WaitForConsumerReserve)

		if err != nil {
			if err.Error() == ErrTimeout.Error() {
				con.OnReserveTimeout()
			} else {
				log.Logger().Error(err)
			}
		} else if id != 0 {
			err = con.handlePayload(id, body)

			if err != nil {
				log.Logger().Error(err)
			}
		}

		select {
		case <-con.quitChannel:
			con.quitChannel <- true
			return
		case taskProcessEvent := <-con.taskEventChannel:
			con.handleTaskEvent(taskProcessEvent)
		case <-time.After(con.config.Heartbeat):
			con.OnHeartbeat()
		}
	}
}

//NewConsumer creates consumer instance with given Handler
func NewConsumer(config *Configuration, handler Handler,
	conn *connector.Connector) *Consumer {
	if config == nil {
		config = NewConfiguration()
	}

	//important to allocate at least one slot to avoid
	//blocking TaskProcessEventHandler while writing to the channel
	var taskEventChannel = make(chan *common.TaskProcessEvent, TaskEventChannelSize)
	var quitChannel = make(chan bool, 1)

	con := &Consumer{config: config, quitChannel: quitChannel,
		taskEventChannel: taskEventChannel, handler: handler}

	conn.SetTaskEventChannel(taskEventChannel)

	return con
}

func (con *Consumer) SetHandler(handler Handler) {
	con.handler = handler
}

func (con *Consumer) SetEventHandler(handler EventHandler) {
	con.eventHandler = handler
}

func (con *Consumer) SetTaskPayloadHandler(handler common.TaskPayloadHandler) {
	con.taskPayloadHandler = handler
}

func NewConfiguration() *Configuration {
	return &Configuration{
		WaitForConsumerReserve: time.Second * 5,
		Heartbeat:              time.Second * 5,
		ReleaseDelay:           time.Second * 5,
		ReleasePriority:        1,
		BuryPriority:           1,
	}
}

//StartConsumer starts consumer thread
func (con *Consumer) StartConsumer() error {
	if util.IsNil(con.handler) {
		return log.MissingConsumerHandlerError()
	}

	if util.IsNil(con.taskPayloadHandler) {
		return log.MissingTaskPayloadHandlerError()
	}

	go func() {
		con.handleConsume()
	}()

	return nil
}

//StopConsumer stops consumer thread
func (con *Consumer) StopConsumer() {
	log.Logger().ConsumerStopping()
	//send stop signal to worker thread
	con.quitChannel <- true

	close(con.taskEventChannel)

	//wait for worker thread stop confirmation
	<-con.quitChannel

	close(con.quitChannel)

	if err := con.Close(); err != nil {
		log.Logger().Error(err)
	}
}

func (con *Consumer) Reserve(timeout time.Duration) (id uint64, body []byte, err error) {
	log.Logger().ConsumerReserve(timeout)

	if !util.IsNil(con.handler) {
		return con.handler.Reserve(timeout)
	}

	return 0, nil, nil
}

func (con *Consumer) Release(id uint64, pri uint32, delay time.Duration) error {
	log.Logger().ConsumerRelease(id, pri, delay)

	if !util.IsNil(con.handler) {
		return con.handler.Release(id, pri, delay)
	}

	return nil
}

func (con *Consumer) Delete(id uint64) error {
	log.Logger().ConsumerDelete(id)

	if !util.IsNil(con.handler) {
		return con.handler.Delete(id)
	}

	return nil
}

func (con *Consumer) Bury(id uint64, pri uint32) error {
	log.Logger().ConsumerBury(id, pri)

	if !util.IsNil(con.handler) {
		return con.handler.Bury(id, pri)
	}

	return nil
}

func (con *Consumer) Touch(id uint64) error {
	log.Logger().ConsumerTouch(id)

	if !util.IsNil(con.handler) {
		return con.handler.Touch(id)
	}

	return nil
}

func (con *Consumer) Close() error {
	log.Logger().ConsumerClose()

	if !util.IsNil(con.handler) {
		return con.handler.Close()
	}

	return nil
}

func (con *Consumer) OnStartConsume() {
	log.Logger().ConsumerStarted()

	if !util.IsNil(con.eventHandler) {
		con.eventHandler.OnStartConsume()
	}
}

func (con *Consumer) OnEndConsume() {
	log.Logger().ConsumerEnded()

	if !util.IsNil(con.eventHandler) {
		con.eventHandler.OnEndConsume()
	}
}

func (con *Consumer) OnReserveTimeout() {
	log.Logger().ConsumerReserveTimeout(con.config.WaitForConsumerReserve)

	if !util.IsNil(con.eventHandler) {
		con.eventHandler.OnReserveTimeout()
	}
}

func (con *Consumer) OnHeartbeat() {
	log.Logger().ConsumerHeartbeat(con.config.Heartbeat)

	if !util.IsNil(con.eventHandler) {
		con.eventHandler.OnHeartbeat()
	}
}
