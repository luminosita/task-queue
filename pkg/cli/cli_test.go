//go:generate mockgen -destination=./mocks/mock_io.go -package=mocks io Writer
package cli_test

import (
	"github.com/golang/mock/gomock"
	"github.com/mnikita/task-queue/pkg/cli"
	"github.com/mnikita/task-queue/pkg/cli/mocks"
	"github.com/mnikita/task-queue/pkg/connection"
	ccmocks "github.com/mnikita/task-queue/pkg/connection/mocks"
	cmocks "github.com/mnikita/task-queue/pkg/consumer/mocks"
	"github.com/mnikita/task-queue/pkg/container"
	lmocks "github.com/mnikita/task-queue/pkg/container/mocks"
	"github.com/mnikita/task-queue/pkg/util"
	wmocks "github.com/mnikita/task-queue/pkg/worker/mocks"
	"github.com/stretchr/testify/assert"
	"os"
	"syscall"
	"testing"
	"time"
)

type Mock struct {
	t *testing.T

	*cli.Configuration

	handler     *lmocks.MockHandler
	connectionH *ccmocks.MockHandler

	ctrl *gomock.Controller

	cli cli.Handler
}

func newMock(t *testing.T, config *cli.Configuration) (m *Mock) {
	m = &Mock{}
	m.t = t
	m.Configuration = config

	m.ctrl = gomock.NewController(t)

	m.handler = lmocks.NewMockHandler(m.ctrl)
	m.connectionH = ccmocks.NewMockHandler(m.ctrl)

	m.cli = cli.NewCli(config, m.handler)

	return
}

func setupTest(m *Mock) func() {
	if m == nil {
		panic("Mock not initialized")
	}

	if m.Configuration != nil {
		m.connectionH.EXPECT().Config().Return(&connection.Configuration{})

		m.handler.EXPECT().Init(gomock.Eq(""))
		m.handler.EXPECT().Connection().Return(m.connectionH)
		m.handler.EXPECT().Close()

		if err := m.cli.Init(); err != nil {
			panic(err)
		}
	}

	return func() {
		defer m.ctrl.Finish()
		defer util.AssertPanic(m.t)

		if m.Configuration != nil {
			if err := m.cli.Close(); err != nil {
				panic(err)
			}
		}
	}
}

func TestMain(m *testing.M) {
	//	log.Logger().Level = logrus.TraceLevel

	wmocks.RegisterTasks()

	os.Exit(m.Run())
}

func TestPut(t *testing.T) {
	var config = cli.NewConfiguration()
	config.Tubes = []string{"default"}
	config.Url = "mock"

	m := newMock(t, config)
	defer setupTest(m)()

	ch := cmocks.NewMockConnectionHandler(m.ctrl)

	bytes := []byte("{\"Name\":\"add\",\"Payload\":\"dGVzdA==\"}")

	m.handler.EXPECT().ConnectionHandler().Return(ch)
	ch.EXPECT().Put(gomock.Eq(bytes), gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(1), nil)

	id, err := m.cli.Put(bytes)

	assert.Equal(t, 1, id)
	assert.Nil(t, err)
}

func TestDelete(t *testing.T) {
	var config = cli.NewConfiguration()
	config.Tubes = []string{"default"}
	config.Url = "mock"

	m := newMock(t, config)
	defer setupTest(m)()

	ch := cmocks.NewMockConnectionHandler(m.ctrl)

	m.handler.EXPECT().ConnectionHandler().Return(ch)
	ch.EXPECT().Delete(uint64(1))

	err := m.cli.Delete(uint64(1))

	assert.Nil(t, err)
}

func TestWriteDefaultConfiguration(t *testing.T) {
	m := newMock(t, nil)
	defer setupTest(m)()

	cc := &container.Configuration{}

	w := mocks.NewMockWriter(m.ctrl)
	m.handler.EXPECT().Config().Return(cc)
	w.EXPECT().Write(gomock.Any()).Return(10, nil)

	n, err := m.cli.WriteDefaultConfiguration(w)

	assert.True(t, n > 0)
	assert.Nil(t, err)
}

func TestStart(t *testing.T) {
	var config = cli.NewConfiguration()
	config.Tubes = []string{"default"}
	config.Url = "mock"

	m := newMock(t, config)
	defer setupTest(m)()

	worker := wmocks.NewMockHandler(m.ctrl)
	consumer := cmocks.NewMockHandler(m.ctrl)

	worker.EXPECT().StartWorker()
	worker.EXPECT().StopWorker()

	consumer.EXPECT().StartConsumer()
	consumer.EXPECT().StopConsumer()

	m.handler.EXPECT().Worker().Return(worker)
	m.handler.EXPECT().Consumer().Return(consumer)

	var ch chan os.Signal

	go func() {
		err := m.cli.Start(func(c chan os.Signal) {
			ch = c
		})

		assert.Nil(t, err)
	}()

	time.Sleep(time.Millisecond * 100)
	ch <- syscall.SIGINT
	time.Sleep(time.Millisecond * 100)
}
