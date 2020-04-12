//go:generate mockgen -destination=./mocks/mock_connection.go -package=mocks . Handler,Dialer,Channels,Channel
package connection

import (
	"github.com/google/wire"
	"github.com/mnikita/task-queue/pkg/consumer"
	"github.com/mnikita/task-queue/pkg/log"
	"github.com/mnikita/task-queue/pkg/util"
	"net/url"
	"time"
)

var WireSet = wire.NewSet(NewConnection, NewConfiguration,
	wire.Bind(new(Handler), new(*Connection)),
	wire.Bind(new(consumer.ConnectionHandler), new(*Connection)))

type Dialer interface {
	Dial(addr string, tubes []string) (consumer.ConnectionHandler, error)

	CreateChannels() Channels
	CreateChannel() Channel
}

type Handler interface {
	Init() error
	Close() error

	Config() *Configuration

	Dialer() Dialer
}

type Configuration struct {
	Tubes []string
	Url   string
}

type Channels interface {
	Reserve(timeout time.Duration) (id uint64, body []byte, err error)
}

type Channel interface {
	Name() string

	Put(body []byte, pri uint32, delay, ttr time.Duration) (id uint64, err error)
}

type Connection struct {
	channels Channels
	channel  Channel
	consumer.ConnectionHandler

	dialer Dialer

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

	c.ConnectionHandler, err = c.dialer.Dial(addr, c.Tubes)

	if err != nil {
		return err
	}

	if c.Tubes != nil && len(c.Tubes) > 0 {
		if len(c.Tubes) > 1 {
			c.channels = c.dialer.CreateChannels()
		} else {
			c.channel = c.dialer.CreateChannel()
		}
	}

	t, err := c.ListTubes()

	if err != nil {
		return err
	}

	log.Logger().BeanConnectionEstablished(t)

	return nil
}

func NewConnection(config *Configuration, dialer Dialer) *Connection {
	connection := &Connection{Configuration: config}

	connection.dialer = dialer

	return connection
}

func NewConfiguration() *Configuration {
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
	return c.ConnectionHandler.Close()
}

func (c *Connection) Config() *Configuration {
	return c.Configuration
}

func (c *Connection) Dialer() Dialer {
	return c.dialer
}

func (c *Connection) Reserve(timeout time.Duration) (id uint64, body []byte, err error) {
	if util.IsNil(c.channels) {
		return c.ConnectionHandler.Reserve(timeout)
	}

	return c.channels.Reserve(timeout)
}

func (c *Connection) Delete(id uint64) (err error) {
	log.Logger().ConsumerDelete(id)

	return c.ConnectionHandler.Delete(id)
}

func (c *Connection) Put(body []byte, pri uint32, delay, ttr time.Duration) (id uint64, err error) {
	if util.IsNil(c.channel) {
		return 0, log.MissingChannel()
	}

	log.Logger().ConsumerPut(c.channel.Name(), pri, delay, ttr)

	return c.channel.Put(body, pri, delay, ttr)
}

func (c *Connection) DefaultTube() (string, error) {
	if util.IsNil(c.channel) {
		return "", log.MissingChannel()
	}

	return c.channel.Name(), nil
}
