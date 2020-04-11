package container_test

import (
	"github.com/golang/mock/gomock"
	bmocks "github.com/mnikita/task-queue/pkg/beanstalkd/mocks"
	connmocks "github.com/mnikita/task-queue/pkg/connector/mocks"
	lmocks "github.com/mnikita/task-queue/pkg/consumer/mocks"
	"github.com/mnikita/task-queue/pkg/container"
	"github.com/mnikita/task-queue/pkg/util"
	wmocks "github.com/mnikita/task-queue/pkg/worker/mocks"
	"testing"
	"time"
)

type Mock struct {
	t *testing.T

	ctrl *gomock.Controller

	cc *container.Configuration

	connectionH *bmocks.MockHandler
	consumerH   *lmocks.MockHandler
	workerH     *wmocks.MockHandler
	connectorH  *connmocks.MockHandler

	container container.Handler
}

func newMock(t *testing.T) *Mock {
	m := &Mock{}
	m.t = t
	m.ctrl = gomock.NewController(t)

	m.connectionH = bmocks.NewMockHandler(m.ctrl)
	m.consumerH = lmocks.NewMockHandler(m.ctrl)
	m.workerH = wmocks.NewMockHandler(m.ctrl)
	m.connectorH = connmocks.NewMockHandler(m.ctrl)

	m.cc = container.NewConfiguration()

	m.container = container.NewContainer(m.cc)

	m.container.SetConnection(m.connectionH)
	m.container.SetWorker(m.workerH)
	m.container.SetConsumer(m.consumerH)
	m.container.SetConnector(m.connectorH)

	return m
}

func setupTest(m *Mock) func() {
	if m == nil {
		panic("Mock not initialized")
	}

	m.connectorH.EXPECT().Init()
	m.workerH.EXPECT().Init()
	m.consumerH.EXPECT().Init()
	m.connectionH.EXPECT().Init()

	m.connectorH.EXPECT().Close()
	m.workerH.EXPECT().Close()
	m.consumerH.EXPECT().Close()
	m.connectionH.EXPECT().Close()

	if err := m.container.Init(""); err != nil {
		panic(err)
	}

	//wait for things to boot up
	time.Sleep(time.Millisecond * 5)

	// Test teardown - return a closure for use by 'defer'
	return func() {
		//wait for task process event
		time.Sleep(time.Millisecond * 30)

		defer m.ctrl.Finish()
		defer util.AssertPanic(m.t)

		//wait for threads to clean up
		time.Sleep(time.Millisecond * 10)
	}
}

func TestStartContainer(t *testing.T) {
	setupTest(newMock(t))
}

//TODO: Integration testing
