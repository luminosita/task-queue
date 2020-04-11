//go:generate mockgen -destination=./mocks/mock_io.go -package=mocks io Writer
package cli_test

import (
	"github.com/golang/mock/gomock"
	"github.com/mnikita/task-queue/pkg/cli"
	"github.com/mnikita/task-queue/pkg/cli/mocks"
	cmocks "github.com/mnikita/task-queue/pkg/consumer/mocks"
	"github.com/mnikita/task-queue/pkg/container"
	lmocks "github.com/mnikita/task-queue/pkg/container/mocks"
	"github.com/mnikita/task-queue/pkg/util"
	wmocks "github.com/mnikita/task-queue/pkg/worker/mocks"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

type Mock struct {
	t *testing.T

	*cli.Configuration

	cc *container.Configuration

	handler *lmocks.MockHandler

	ctrl *gomock.Controller

	cli cli.Handler
}

func newMock(t *testing.T, config *cli.Configuration) (m *Mock) {
	m = &Mock{}
	m.t = t
	m.Configuration = config

	m.cc = container.NewConfiguration()

	m.ctrl = gomock.NewController(t)

	m.handler = lmocks.NewMockHandler(m.ctrl)

	m.cli = cli.NewCli(config)
	m.cli.SetContainerHandler(m.handler)

	return
}

func setupTest(m *Mock) func() {
	if m == nil {
		panic("Mock not initialized")
	}

	if m.Configuration != nil {
		m.handler.EXPECT().Init(gomock.Eq(""))
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

	os.Exit(m.Run())
}

func TestPut(t *testing.T) {
	var config = &cli.Configuration{
		Tubes: []string{"default"},
		Url:   "mock",
	}

	m := newMock(t, config)
	defer setupTest(m)()

	ch := cmocks.NewMockConnectionHandler(m.ctrl)

	bytes := []byte("{\"Name\":\"add\",\"Payload\":\"dGVzdA==\"}")

	m.handler.EXPECT().ConsumerConnectionHandler().Return(ch)
	ch.EXPECT().Put(gomock.Eq(bytes), gomock.Any(), gomock.Any(), gomock.Any()).Return(uint64(0), nil)

	err := m.cli.Put(bytes)

	assert.Nil(t, err)
}

func TestWriteDefaultConfiguration(t *testing.T) {
	m := newMock(t, nil)
	defer setupTest(m)()

	w := mocks.NewMockWriter(m.ctrl)
	m.handler.EXPECT().Config().Return(m.cc)
	w.EXPECT().Write(gomock.Any()).Return(10, nil)

	n, err := m.cli.WriteDefaultConfiguration(w)

	assert.True(t, n > 0)
	assert.Nil(t, err)
}

func TestStart(t *testing.T) {
	var config = &cli.Configuration{
		Tubes: []string{"default"},
		Url:   "mock",
	}

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

	err := m.cli.Start(false)

	assert.Nil(t, err)
}
