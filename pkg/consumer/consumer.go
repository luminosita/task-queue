package consumer

import (
	"encoding/json"
	"errors"
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/mnikita/task-queue/pkg/log"
	"time"
)

//Consumer stores configuration for consumer activation
type Consumer struct {
	Handler

	EventHandler

	taskPayloadHandler common.TaskPayloadHandler

	config *Configuration

	quit chan bool

	//	Tube
	//	TubeSet
}

//Configuration stores initialization data for worker server
type Configuration struct {
	//Waiting time for consumer reserve
	WaitForConsumerReserve time.Duration

	//Waiting time for quit signal timeout
	QuitSignalTimeout time.Duration
}

var ErrTimeout = errors.New("timeout")

type Handler interface {
	Reserve(timeout time.Duration) (id uint64, body []byte, err error)
	Release(id uint64, pri uint32, delay time.Duration) error
	Delete(id uint64) error
	Close() error
}

//HandlePayload unmarshal payload data into Task instance to invoke given TaskPayloadHandler
func (con *Consumer) handlePayload(body []byte) error {
	if body != nil {
		task := &common.Task{}

		err := json.Unmarshal(body, task)

		if err != nil {
			return err
		}

		con.taskPayloadHandler.HandlePayload(task)
	}

	return nil
}

func (con *Consumer) handleConsume() {
	con.OnStartConsume()

	defer con.OnEndConsume()

	var runLoop = true

	for runLoop {
		id, body, err := con.Reserve(con.config.WaitForConsumerReserve)

		if err != nil {
			if err == ErrTimeout {
				con.OnReserveTimeout()
			}

			log.Logger().Error(err)
		} else {
			err = con.handlePayload(body)

			if err != nil {
				log.Logger().Error(err)

				err1 := con.Release(id, 0, 0)

				if err1 != nil {
					log.Logger().Error(err1)
				}
			}

			err = con.Delete(id)

			if err != nil {
				log.Logger().Error(err)
			}
		}

		select {
		case <-con.quit:
			con.quit <- true
			runLoop = false
		case <-time.After(con.config.QuitSignalTimeout):
			con.OnQuitSignalTimeout()
		}
	}
}

//NewConsumer creates consumer instance with given Handler
func NewConsumer(consumerHandler Handler, eventHandler EventHandler,
	payloadHandler common.TaskPayloadHandler) *Consumer {

	con := &Consumer{}

	con.Handler = consumerHandler
	con.EventHandler = eventHandler
	con.taskPayloadHandler = payloadHandler

	return con
}

//LoadConfiguration loads external confirmation
func LoadConfiguration(configData []byte) (config *Configuration, err error) {
	config = NewConfiguration()

	err = json.Unmarshal(configData, config)

	if err != nil {
		return nil, err
	}

	return config, nil
}

func NewConfiguration() *Configuration {
	return &Configuration{
		WaitForConsumerReserve: time.Second * 5,
		QuitSignalTimeout:      time.Second * 5,
	}
}

//StartConsumer starts consumer thread
func (con *Consumer) StartConsumer() {
	log.Logger().ConsumerStarted()

	defer func() {
		if err := con.Close(); err != nil {
			log.Logger().Error(err)
		}
	}()

	//make default configuration
	if con.config == nil {
		con.config = NewConfiguration()
	}

	con.quit = make(chan bool)

	go func() {
		con.handleConsume()
	}()
}

//StopConsumer stops consumer thread
func (con *Consumer) StopConsumer() {
	log.Logger().ConsumerStopping()
	//send stop signal to worker thread
	con.quit <- true

	//wait for worker thread stop confirmation
	<-con.quit

	log.Logger().ConsumerEnded()
}
