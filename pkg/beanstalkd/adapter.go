package beanstalkd

import (
	gob "github.com/beanstalkd/go-beanstalk"
	"github.com/google/wire"
	"github.com/mnikita/task-queue/pkg/connection"
	"github.com/mnikita/task-queue/pkg/consumer"
)

var WireSet = wire.NewSet(NewDialer, NewConfiguration)

const NetworkTcp = "tcp"

type Configuration struct {
	Addr  string
	Tubes []string
}

type BeanstakldDialer struct {
	*Configuration

	handler consumer.ConnectionHandler
}

func NewConfiguration() *Configuration {
	return &Configuration{}
}

func NewDialer(config *Configuration) connection.Dialer {
	return &BeanstakldDialer{Configuration: config}
}

func (b *BeanstakldDialer) Dial(addr string, tubes []string) (consumer.ConnectionHandler, error) {
	//check for ENV
	b.Addr = addr
	b.Tubes = tubes

	return gob.DialTimeout(NetworkTcp, addr, gob.DefaultDialTimeout)
}

func (b *BeanstakldDialer) CreateChannels() connection.Channels {
	return gob.NewTubeSet(b.handler.(*gob.Conn), b.Tubes...)
}

func (b *BeanstakldDialer) CreateChannel() connection.Channel {
	return &gob.Tube{Conn: b.handler.(*gob.Conn), Name: b.Tubes[0]}
}
