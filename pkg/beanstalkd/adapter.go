package beanstalkd

import (
	gob "github.com/beanstalkd/go-beanstalk"
	"github.com/mnikita/task-queue/pkg/connection"
	"github.com/mnikita/task-queue/pkg/consumer"
)

const NetworkTcp = "tcp"

type Configuration struct {
	Tubes []string
}

type BeanstakldDialer struct {
	*Configuration

	handler consumer.ConnectionHandler
}

func NewConfiguration() *Configuration {
	return &Configuration{}
}

func NewDialer(config *Configuration) *BeanstakldDialer {
	return &BeanstakldDialer{Configuration: config}
}

func (b *BeanstakldDialer) Dial(addr string, tubes []string) (consumer.ConnectionHandler, error) {
	b.Tubes = tubes

	return gob.DialTimeout(NetworkTcp, addr, gob.DefaultDialTimeout)
}

func (b *BeanstakldDialer) CreateChannels() connection.Channels {
	return gob.NewTubeSet(b.handler.(*gob.Conn), b.Tubes...)
}

func (b *BeanstakldDialer) CreateChannel() connection.Channel {
	return &gob.Tube{Conn: b.handler.(*gob.Conn), Name: b.Tubes[0]}
}
