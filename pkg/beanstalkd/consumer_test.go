package beanstalkd

import (
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

type MockConnection struct {
	t *testing.T

	config *Configuration

	returnReserveId    uint64
	returnReserveBody  []byte
	returnReserveError error

	expectReleasePri   uint32
	expectReleaseDelay time.Duration
	returnReleaseError error

	returnDeleteError error
	returnCloseError  error

	expectTask *common.Task

	startConsumerFlag int
	endConsumerFlag   int
	quitSignalFlag    int
}

func (conn *MockConnection) OnStartConsume() {
	conn.startConsumerFlag--
}

func (conn *MockConnection) OnEndConsume() {
	conn.endConsumerFlag--
}

func (conn *MockConnection) OnQuitSignalTimeout() {
	conn.quitSignalFlag--
}

func (conn *MockConnection) Reserve(timeout time.Duration) (id uint64, body []byte, err error) {
	if conn.config != nil {
		assert.Equal(conn.t, conn.config.WaitForConsumerReserve, timeout)
	}

	return conn.returnReserveId, conn.returnReserveBody, conn.returnReserveError
}

func (conn *MockConnection) Release(id uint64, pri uint32, delay time.Duration) error {
	assert.Equal(conn.t, conn.returnReserveId, id)
	assert.Equal(conn.t, conn.expectReleasePri, pri)
	assert.Equal(conn.t, conn.expectReleaseDelay, delay)

	return conn.returnReleaseError
}

func (conn *MockConnection) Delete(id uint64) error {
	assert.Equal(conn.t, conn.returnReserveId, id)

	return conn.returnDeleteError
}

func (conn *MockConnection) Close() error {
	return conn.returnCloseError
}

func (conn *MockConnection) HandlePayload(task *common.Task) {
	if conn.expectTask != nil {
		assert.Equal(conn.t, conn.expectTask, task)
	}
}

func setupTest(t *testing.T, conn *MockConnection) func() {
	//log.Logger().Level = logrus.TraceLevel
	// Test setup
	conn.t = t

	c := NewConsumer(conn)

	c.config = conn.config

	c.StartConsumer(conn)

	//wait for things to boot up
	time.Sleep(time.Millisecond * 5)

	// Test teardown - return a closure for use by 'defer'
	return func() {
		c.StopConsumer()

		//wait for threads to clean up
		time.Sleep(time.Millisecond)

		assert.Equal(t, 0, conn.startConsumerFlag)
		assert.Equal(t, 0, conn.endConsumerFlag)
		assert.Equal(t, 0, conn.quitSignalFlag)
	}
}

func TestStartConsumer(t *testing.T) {
	conn := &MockConnection{config: nil, returnReserveBody: []byte("{}"),
		startConsumerFlag: 1, endConsumerFlag: 1, quitSignalFlag: 0}

	defer setupTest(t, conn)()
}

func TestReserveTask(t *testing.T) {
	config := NewConfiguration()
	config.WaitForConsumerReserve = time.Second * 7
	config.QuitSignalTimeout = time.Second * 5

	conn := &MockConnection{
		config: config,

		returnReserveId:   13,
		returnReserveBody: []byte("{\"Name\":\"add\",\"Payload\":\"dGVzdA==\"}"),

		expectTask:        &common.Task{Name: "add", Payload: []byte("test")},
		startConsumerFlag: 1, endConsumerFlag: 1, quitSignalFlag: 0,
	}

	defer setupTest(t, conn)()
}

func TestQuitSignal(t *testing.T) {
	config := NewConfiguration()
	config.WaitForConsumerReserve = time.Second * 1
	//setting quit signal timeout slightly shorter than final Sleep time
	config.QuitSignalTimeout = time.Millisecond * 35

	conn := &MockConnection{
		config:            config,
		returnReserveBody: []byte("{}"),
		startConsumerFlag: 1, endConsumerFlag: 1, quitSignalFlag: 1}

	defer setupTest(t, conn)()

	time.Sleep(time.Millisecond * 40)
}
