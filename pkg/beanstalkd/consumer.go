package beanstalkd

import (
	"encoding/json"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/mnikita/task-queue/pkg/log"
	"time"
)

type Connection struct {
	*beanstalk.Conn
}

//Consumer stores configuration for consumer activation
type Consumer struct {
	common.ConsumerHandler

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

//HandlePayload unmarshal payload data into Task instance to invoke given TaskPayloadHandler
func (con *Consumer) handlePayload(handler common.TaskPayloadHandler, body []byte) error {
	task := &common.Task{}

	err := json.Unmarshal(body, task)

	if err != nil {
		return err
	}

	handler.HandlePayload(task)

	return nil
}

//NewConsumer creates consumer instance with given ConsumerHandler
func NewConsumer(handler common.ConsumerHandler) *Consumer {
	con := &Consumer{}

	con.ConsumerHandler = handler

	return con
}

// Dial connects addr on the given network using net.DialTimeout
// with a default timeout of 10s and then returns a new Conn for the connection.
func Dial(network, addr string) (*Consumer, error) {
	return DialTimeout(network, addr, beanstalk.DefaultDialTimeout)
}

// DialTimeout connects addr on the given network using net.DialTimeout
// with a supplied timeout and then returns a new Conn for the connection.
func DialTimeout(network, addr string, timeout time.Duration) (*Consumer, error) {
	bConn, err := beanstalk.DialTimeout(network, addr, timeout)

	if err != nil {
		return nil, err
	}

	conn := &Connection{bConn}

	return NewConsumer(conn), nil
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

func (con *Consumer) handleConsume(handler common.TaskPayloadHandler) {
	handler.OnStartConsume()

	defer handler.OnEndConsume()

mainLoop:
	for {
		id, body, err := con.Reserve(con.config.WaitForConsumerReserve)

		if err != nil {
			log.Logger().Error(err)
		}

		err = con.handlePayload(handler, body)

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

		select {
		case <-con.quit:
			con.quit <- true
			break mainLoop
		case <-time.After(con.config.QuitSignalTimeout):
			handler.OnQuitSignalTimeout()
		}
	}
}

//StartConsumer starts consumer thread
func (con *Consumer) StartConsumer(handler common.TaskPayloadHandler) {
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
		con.handleConsume(handler)
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

//var conn, _ = beanstalk.Dial("tcp", "127.0.0.1:11300")
//
//func Example_reserve() {
//	id, body, err := conn.Reserve(5 * time.Second)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("job", id)
//	fmt.Println(string(body))
//}
//
//func Example_reserveOtherTubeSet() {
//	tubeSet := beanstalk.NewTubeSet(conn, "mytube1", "mytube2")
//	id, body, err := tubeSet.Reserve(10 * time.Hour)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("job", id)
//	fmt.Println(string(body))
//}
//
//func Example_put() {
//	id, err := conn.Put([]byte("myjob"), 1, 0, time.Minute)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("job", id)
//}
//
//func Example_putOtherTube() {
//	tube := &beanstalk.Tube{Conn: conn, Name: "mytube"}
//	id, err := tube.Put([]byte("myjob"), 1, 0, time.Minute)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("job", id)
//}
