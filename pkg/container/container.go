//go:generate mockgen -destination=./mocks/mock_container.go -package=mocks . Handler
//Package container provides primitives for configuration and starting all primitives in the module
package container

import (
	"encoding/json"
	"github.com/mnikita/task-queue/pkg/beanstalkd"
	"github.com/mnikita/task-queue/pkg/connector"
	"github.com/mnikita/task-queue/pkg/consumer"
	"github.com/mnikita/task-queue/pkg/log"
	"github.com/mnikita/task-queue/pkg/util"
	"github.com/mnikita/task-queue/pkg/worker"
	"io/ioutil"
	"os"
)

type Handler interface {
	Init(configFile string) error
	Close() error

	Connection() beanstalkd.Handler
	Worker() worker.Handler
	Consumer() consumer.Handler
	Connector() connector.Handler

	SetConnector(connector connector.Handler)
	SetConsumer(consumer consumer.Handler)
	SetConnection(connection beanstalkd.Handler)
	SetWorker(worker worker.Handler)

	Config() *Configuration
}

type Configuration struct {
	ConnectionConfig *beanstalkd.Configuration
	WorkerConfig     *worker.Configuration
	ConsumerConfig   *consumer.Configuration
	ConnectorConfig  *connector.Configuration

	ConfigFile string `json:"-"`

	configWatcher *util.ConfigWatcher `json:"-"`
}

type Container struct {
	*Configuration

	connection beanstalkd.Handler
	worker     worker.Handler
	consumer   consumer.Handler
	connector  connector.Handler
}

func (c *Configuration) load() error {
	if c.ConfigFile == "" {
		//nothing to load
		return nil
	}

	jsonFile, err := os.Open(c.ConfigFile)
	if err != nil {
		return err
	}

	configData, err := ioutil.ReadAll(jsonFile)

	if err != nil {
		return err
	}

	err = json.Unmarshal(configData, c)

	if err != nil {
		return err
	}

	log.Logger().ContainerConfigLoaded(c.ConfigFile)

	err = c.initConfigWatcher()
	if err != nil {
		return err
	}

	return nil
}

func (c *Configuration) close() error {
	if c.ConfigFile == "" {
		//nothing to close
		return nil
	}

	return c.closeConfigWatcher()
}

func (c *Configuration) initConfigWatcher() (err error) {
	c.configWatcher, err = util.NewConfigWatcher(c)

	if err != nil {
		return err
	}

	err = c.configWatcher.WatchConfigFile(c.ConfigFile)

	if err != nil {
		return err
	}

	return nil
}

func (c *Configuration) closeConfigWatcher() (err error) {
	return c.configWatcher.StopWatch()
}

func NewConfiguration() *Configuration {
	return &Configuration{}
}

//TODO: Lazy load with singletons
//TODO:(consumer and worker tests are not using all part of container). Waste of time to initialize everything
func NewContainer(config *Configuration) (c *Container) {
	c = &Container{}

	c.Configuration = config

	if c.ConnectionConfig != nil {
		c.connection = beanstalkd.NewConnection(c.ConnectionConfig, nil)
	}
	if c.ConnectorConfig != nil {
		c.connector = connector.NewConnector(c.ConnectorConfig)
	}
	if c.WorkerConfig != nil {
		c.worker = worker.NewWorker(c.WorkerConfig, c.connector)
	}
	if c.ConsumerConfig != nil {
		c.consumer = consumer.NewConsumer(c.ConsumerConfig,
			c.connector, c.connection.(consumer.ConnectionHandler))
	}

	return c
}

func (c *Container) Init(configFile string) (err error) {
	//Init Configuration
	c.ConfigFile = configFile

	if err = c.load(); err != nil {
		return err
	}

	//Init Objects
	if !util.IsNil(c.Connection()) {
		if err = c.Connection().Init(); err != nil {
			return err
		}
	}
	if !util.IsNil(c.Worker()) {
		if err = c.Worker().Init(); err != nil {
			return err
		}
	}
	if !util.IsNil(c.Consumer()) {
		if err = c.Consumer().Init(); err != nil {
			return err
		}
	}
	if !util.IsNil(c.Connector()) {
		if err = c.Connector().Init(); err != nil {
			return err
		}
	}

	return nil
}

func (c *Container) Close() (err error) {
	//Close Objects
	if !util.IsNil(c.Connection()) {
		err = c.Connector().Close()
		if err != nil {
			return err
		}
	}
	if !util.IsNil(c.Consumer()) {
		err = c.Consumer().Close()
		if err != nil {
			return err
		}
	}
	if !util.IsNil(c.Worker()) {
		err = c.Worker().Close()
		if err != nil {
			return err
		}
	}
	if !util.IsNil(c.Connection()) {
		err = c.Connection().Close()
		if err != nil {
			return err
		}
	}
	//Close Configuration
	return c.close()
}

func (c *Container) Connection() beanstalkd.Handler {
	return c.connection
}

func (c *Container) Worker() worker.Handler {
	return c.worker
}

func (c *Container) Consumer() consumer.Handler {
	return c.consumer
}

func (c *Container) Connector() connector.Handler {
	return c.connector
}

func (c *Container) SetConnector(connector connector.Handler) {
	c.connector = connector
}

func (c *Container) SetConsumer(consumer consumer.Handler) {
	c.consumer = consumer
}

func (c *Container) SetConnection(connection beanstalkd.Handler) {
	c.connection = connection
}

func (c *Container) SetWorker(worker worker.Handler) {
	c.worker = worker
}

func (c *Container) Config() *Configuration {
	return c.Configuration
}

func (c *Configuration) OnConfigModified() {
	//TODO: Implement configuration modification logic
}
