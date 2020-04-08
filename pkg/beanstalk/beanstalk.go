package beanstalk

import (
	"encoding/json"
	gob "github.com/beanstalkd/go-beanstalk"
	"github.com/mnikita/task-queue/pkg/connector"
	"github.com/mnikita/task-queue/pkg/consumer"
	"github.com/mnikita/task-queue/pkg/log"
	"github.com/mnikita/task-queue/pkg/util"
	"github.com/mnikita/task-queue/pkg/worker"
	"io/ioutil"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type ServerConfiguration struct {
	Worker    *worker.Configuration
	Consumer  *consumer.Configuration
	Connector *connector.Configuration
}

type Configuration struct {
	Tubes      []string
	Url        string
	ConfigFile string `json:"-"`

	*ServerConfiguration

	configWatcher *util.ConfigWatcher
}

type TubeSetAdapter struct {
	*gob.TubeSet
	*gob.Conn
}

func parseUrl(urlText string) (protocol string, addr string, err error) {
	serverUrl, err := url.Parse(urlText)

	if err != nil {
		return "", "", err
	}

	protocol = serverUrl.Scheme
	addr = serverUrl.Host

	return protocol, addr, nil
}

func (c *Configuration) loadConfiguration() error {
	serverConfig := newConfiguration()
	c.ServerConfiguration = serverConfig

	if c.ConfigFile == "" {
		return nil
	}

	jsonFile, err := os.Open(c.ConfigFile)
	if err != nil {
		log.Logger().Fatal(err)
	}

	configData, err := ioutil.ReadAll(jsonFile)

	if err != nil {
		return err
	}

	err = json.Unmarshal(configData, c)

	if err != nil {
		return err
	}

	log.Logger().BeanConfigLoaded(c.ConfigFile)

	return nil
}

func (c *Configuration) OnConfigModified() {
	//TODO: Implement configuration modification logic
}

func establishConnection(config *Configuration) (*TubeSetAdapter, error) {
	err := config.loadConfiguration()

	if err != nil {
		return nil, err
	}

	network, addr, err := parseUrl(config.Url)

	if err != nil {
		return nil, err
	}

	log.Logger().BeanUrl(config.Url)

	bConn, err := gob.DialTimeout(network, addr, gob.DefaultDialTimeout)

	if err != nil {
		return nil, err
	}

	var tubeSetAdapter *TubeSetAdapter

	if config.Tubes != nil {
		tubeSetAdapter = &TubeSetAdapter{TubeSet: gob.NewTubeSet(bConn, config.Tubes...), Conn: bConn}
	} else {
		tubeSetAdapter = &TubeSetAdapter{Conn: bConn}
	}

	t, err := bConn.ListTubes()

	if err != nil {
		return nil, err
	}

	log.Logger().BeanConnectionEstablished(t)

	return tubeSetAdapter, nil
}

func Put(config *Configuration) (err error) {
	tubeSetAdapter, err := establishConnection(config)

	if err != nil {
		return err
	}

	defer func() {
		err = tubeSetAdapter.Close()
	}()

	//TODO: Put task data read from file
	_, err = tubeSetAdapter.Put([]byte("add"), 1, 0, time.Minute)

	if err != nil {
		return err
	}

	return nil
}

func Start(config *Configuration) (err error) {
	var tubeSetAdapter *TubeSetAdapter

	tubeSetAdapter, err = establishConnection(config)

	if err != nil {
		return err
	}

	defer func() {
		err = tubeSetAdapter.Close()
	}()

	conn := connector.NewConnector(config.Connector)

	w := worker.NewWorker(config.Worker, conn)
	c := consumer.NewConsumer(config.Consumer, tubeSetAdapter, conn)

	c.SetTaskPayloadHandler(conn)
	w.SetTaskEventHandler(conn)

	w.StartWorker()
	err = c.StartConsumer()

	if err != nil {
		return err
	}

	defer c.StopConsumer()
	defer w.StopWorker()

	config.configWatcher, err = util.NewConfigWatcher(config)
	defer func() {
		err = config.configWatcher.StopWatch()
	}()

	if err != nil {
		return err
	}

	err = config.configWatcher.WatchConfigFile(config.ConfigFile)

	if err != nil {
		return err
	}

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		done <- true
	}()

	<-done

	return nil
}

func newConfiguration() *ServerConfiguration {
	return &ServerConfiguration{
		Worker:    worker.NewConfiguration(),
		Consumer:  consumer.NewConfiguration(),
		Connector: connector.NewConfiguration(),
	}
}

func WriteDefaultConfiguration(filePath string) error {
	config := &Configuration{
		Url:                 "tcp://127.0.0.1:11300",
		Tubes:               []string{"default"},
		ServerConfiguration: newConfiguration(),
	}

	bytes, err := json.MarshalIndent(config, "", " ")

	if err != nil {
		return err
	}

	return ioutil.WriteFile(filePath, bytes, 0644)
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
