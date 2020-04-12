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

	handler *gob.Conn
}

type TubeAdapter struct {
	*gob.Tube
}

func NewConfiguration() *Configuration {
	return &Configuration{}
}

func NewDialer(config *Configuration) connection.Dialer {
	return &BeanstakldDialer{Configuration: config}
}

func (ta *TubeAdapter) Name() string {
	return ta.Tube.Name
}

func (b *BeanstakldDialer) Dial(addr string, tubes []string) (consumer.ConnectionHandler, error) {
	//check for ENV
	b.Addr = addr
	b.Tubes = tubes

	var err error

	b.handler, err = gob.DialTimeout(NetworkTcp, addr, gob.DefaultDialTimeout)

	return b.handler, err
}

func (b *BeanstakldDialer) CreateChannels() connection.Channels {
	return gob.NewTubeSet(b.handler, b.Tubes...)
}

func (b *BeanstakldDialer) CreateChannel() connection.Channel {
	return &TubeAdapter{Tube: &gob.Tube{Conn: b.handler, Name: b.Tubes[0]}}
}
