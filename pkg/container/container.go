//go:generate mockgen -destination=./mocks/mock_container.go -package=mocks . Handler
//Package container provides primitives for configuration and starting all primitives in the module
package container

import (
	"encoding/json"
	"github.com/mnikita/task-queue/pkg/connection"
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

	Connection() connection.Handler
	Worker() worker.Handler
	Consumer() consumer.Handler
	Connector() connector.Handler

	SetConnector(connector connector.Handler)
	SetConsumer(consumer consumer.Handler)
	SetConnection(connection connection.Handler)
	SetWorker(worker worker.Handler)

	Config() *Configuration
}

type Configuration struct {
	ConnectionConfig *connection.Configuration
	WorkerConfig     *worker.Configuration
	ConsumerConfig   *consumer.Configuration
	ConnectorConfig  *connector.Configuration

	ConfigFile string `json:"-"`

	configWatcher *util.ConfigWatcher `json:"-"`
}

type Container struct {
	*Configuration

	connection connection.Handler
	worker     worker.Handler
	consumer   consumer.Handler
	connector  connector.Handler
	dialer     connection.Dialer
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
	config := &Configuration{}
	config.WorkerConfig = worker.NewConfiguration()
	config.ConsumerConfig = consumer.NewConfiguration()
	config.ConnectorConfig = connector.NewConfiguration()
	config.ConnectionConfig = connection.NewConfiguration()

	return config
}

//TODO: Lazy load with singletons
//TODO:(consumer and worker tests are not using all part of container). Waste of time to initialize everything
func NewContainer(config *Configuration, dialer connection.Dialer) (c *Container) {
	c = &Container{}

	c.Configuration = config

	c.dialer = dialer

	//Skip all constructors
	if config == nil {
		//setting empty configuration
		c.Configuration = &Configuration{}

		return
	}

	c.connection = connection.NewConnection(c.ConnectionConfig, c.dialer)
	c.connector = connector.NewConnector(c.ConnectorConfig)
	c.worker = worker.NewWorker(c.WorkerConfig, c.connector)
	c.consumer = consumer.NewConsumer(c.ConsumerConfig,
		c.connector, c.connection.(consumer.ConnectionHandler))

	return
}

func (c *Container) Init(configFile string) (err error) {
	//Init Configuration
	c.ConfigFile = configFile

	if err = c.load(); err != nil {
		return err
	}

	//Init Objects
	if err = c.Connection().Init(); err != nil {
		return err
	}
	if err = c.Worker().Init(); err != nil {
		return err
	}
	if err = c.Consumer().Init(); err != nil {
		return err
	}
	if err = c.Connector().Init(); err != nil {
		return err
	}

	return nil
}

func (c *Container) Close() (err error) {
	//Close Objects
	err = c.Connector().Close()
	if err != nil {
		return err
	}
	err = c.Consumer().Close()
	if err != nil {
		return err
	}
	err = c.Worker().Close()
	if err != nil {
		return err
	}
	err = c.Connection().Close()
	if err != nil {
		return err
	}

	//Close Configuration
	return c.close()
}

func (c *Container) Connection() connection.Handler {
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

func (c *Container) SetConnection(connection connection.Handler) {
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
