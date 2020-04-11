//go:generate mockgen -destination=./mocks/mock_connection.go -package=mocks . Handler,Dialer
package beanstalkd

import (
	gob "github.com/beanstalkd/go-beanstalk"
	"github.com/mnikita/task-queue/pkg/consumer"
	"github.com/mnikita/task-queue/pkg/log"
	"github.com/mnikita/task-queue/pkg/util"
	"net/url"
	"time"
)

const NetworkTcp = "tcp"

type Dialer interface {
	Dial(addr string) (consumer.ConnectionHandler, error)

	CreateTubeSet() *gob.TubeSet
	CreateTube() *gob.Tube
}

type Handler interface {
	Init() error
	Close() error

	Dialer() Dialer
}

type Configuration struct {
	Tubes []string
	Url   string

	dialer Dialer
}

type Connection struct {
	*gob.TubeSet
	*gob.Tube

	ch consumer.ConnectionHandler

	*Configuration
}

func parseUrl(urlText string) (addr string, err error) {
	serverUrl, err := url.Parse(urlText)

	if err != nil {
		return "", err
	}

	addr = serverUrl.Host

	return addr, nil
}

func (c *Connection) establishConnection() error {
	addr, err := parseUrl(c.Url)

	if err != nil {
		return err
	}

	log.Logger().BeanUrl(c.Url)

	c.ch, err = c.dialer.Dial(addr)

	if err != nil {
		return err
	}

	if c.Tubes != nil {
		if len(c.Tubes) > 1 {
			c.TubeSet = c.dialer.CreateTubeSet()
		} else {
			c.Tube = c.dialer.CreateTube()
		}
	}

	t, err := c.ch.ListTubes()

	if err != nil {
		return err
	}

	log.Logger().BeanConnectionEstablished(t)

	return nil
}

func NewConnection(config *Configuration, dialer Dialer) (connection *Connection) {
	connection = &Connection{Configuration: config}

	connection.dialer = connection

	if !util.IsNil(dialer) {
		connection.dialer = dialer
	}

	return connection
}

func NewConfiguration() *Configuration {
	//make default configuration
	return &Configuration{}
}

func (c *Connection) Init() (err error) {
	err = c.establishConnection()

	if err != nil {
		return err
	}

	return nil
}

func (c *Connection) Close() error {
	return c.ch.Close()
}

func (c *Connection) Dialer() Dialer {
	return c.dialer
}

func (c *Connection) Dial(addr string) (consumer.ConnectionHandler, error) {
	return gob.DialTimeout(NetworkTcp, addr, gob.DefaultDialTimeout)
}

func (c *Connection) CreateTubeSet() *gob.TubeSet {
	return gob.NewTubeSet(c.ch.(*gob.Conn), c.Tubes...)
}

func (c *Connection) CreateTube() *gob.Tube {
	return &gob.Tube{Conn: c.ch.(*gob.Conn), Name: c.Tubes[0]}
}

func (c *Connection) Reserve(timeout time.Duration) (id uint64, body []byte, err error) {
	if util.IsNil(c.TubeSet) {
		return c.ch.Reserve(timeout)
	}

	return c.TubeSet.Reserve(timeout)
}
