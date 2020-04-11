package beanstalkd_test

import (
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/mnikita/task-queue/pkg/beanstalkd"
	"github.com/mnikita/task-queue/pkg/beanstalkd/mocks"
	cmocks "github.com/mnikita/task-queue/pkg/consumer/mocks"
	"github.com/mnikita/task-queue/pkg/util"
	"github.com/stretchr/testify/assert"
	"testing"
)

type Mock struct {
	t *testing.T

	bc *beanstalkd.Configuration

	ctrl *gomock.Controller

	handler beanstalkd.Handler

	dialer *mocks.MockDialer
	conn   *cmocks.MockConnectionHandler
}

func newMock(t *testing.T) *Mock {
	m := &Mock{}
	m.t = t
	m.ctrl = gomock.NewController(t)

	m.dialer = mocks.NewMockDialer(m.ctrl)
	m.conn = cmocks.NewMockConnectionHandler(m.ctrl)

	m.bc = beanstalkd.NewConfiguration()

	m.handler = beanstalkd.NewConnection(m.bc, m.dialer)

	return m
}

func setupTest(m *Mock) func() {
	if m == nil {
		panic("Mock not initialized")
	}

	// Test teardown - return a closure for use by 'defer'
	return func() {
		defer m.ctrl.Finish()
		defer util.AssertPanic(m.t)
	}
}

func TestInitConnection(t *testing.T) {
	m := newMock(t)

	m.bc.Url = "tcp://127.0.0.1:11300"

	m.dialer.EXPECT().Dial(gomock.Eq("127.0.0.1:11300")).Return(m.conn, nil)
	m.conn.EXPECT().ListTubes().Return([]string{"default"}, nil)
	m.conn.EXPECT().Close()

	defer setupTest(m)()

	assert.Nil(t, m.handler.Init())

	assert.Nil(t, m.conn.Close())
}

func TestBadUrl(t *testing.T) {
	m := newMock(t)

	m.bc.Url = "http"

	m.dialer.EXPECT().Dial(gomock.Eq("")).Return(nil, errors.New("bad url"))

	defer setupTest(m)()

	assert.NotNil(t, m.handler.Init())
}

func TestEmptyUrl(t *testing.T) {
	m := newMock(t)

	m.bc.Url = ""

	m.dialer.EXPECT().Dial(gomock.Eq("")).Return(nil, errors.New("bad url"))

	defer setupTest(m)()

	assert.NotNil(t, m.handler.Init())
}

func TestTubeSet(t *testing.T) {
	m := newMock(t)

	m.bc.Url = "tcp://127.0.0.1:11300"
	m.bc.Tubes = []string{"mika", "pera", "laza"}

	m.dialer.EXPECT().Dial(gomock.Eq("127.0.0.1:11300")).Return(m.conn, nil)
	m.dialer.EXPECT().CreateTubeSet().Return(nil)
	m.conn.EXPECT().ListTubes().Return([]string{"mika", "pera", "laza"}, nil)
	m.conn.EXPECT().Close()

	defer setupTest(m)()

	assert.Nil(t, m.handler.Init())

	assert.Nil(t, m.conn.Close())
}

func TestTube(t *testing.T) {
	m := newMock(t)

	m.bc.Url = "tcp://127.0.0.1:11300"
	m.bc.Tubes = []string{"mika"}

	m.dialer.EXPECT().Dial(gomock.Eq("127.0.0.1:11300")).Return(m.conn, nil)
	m.dialer.EXPECT().CreateTube().Return(nil)
	m.conn.EXPECT().ListTubes().Return([]string{"mika"}, nil)
	m.conn.EXPECT().Close()

	defer setupTest(m)()

	assert.Nil(t, m.handler.Init())

	assert.Nil(t, m.conn.Close())
}

//TODO: Test Reserve on TubeSet vs Conn
//TODO: Test Put on Tube vs Conn
