package consumer

import (
	"github.com/beanstalkd/go-beanstalk"
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/mnikita/task-queue/pkg/util"
	"github.com/stretchr/testify/assert"
	"reflect"
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

	*Flags
}

type Flags struct {
	StartConsumerFlag int
	EndConsumerFlag   int
	QuitSignalFlag    int

	ReserveFlag        int
	ReleaseFlag        int
	DeleteFlag         int
	CloseFlag          int
	HandlePayloadFlag  int
	ReserveTimeoutFlag int
}

func (conn *MockConnection) OnReserveTimeout() {
	conn.ReserveTimeoutFlag--
}

func (conn *MockConnection) OnStartConsume() {
	conn.StartConsumerFlag--
}

func (conn *MockConnection) OnEndConsume() {
	conn.EndConsumerFlag--
}

func (conn *MockConnection) OnQuitSignalTimeout() {
	conn.QuitSignalFlag--
}

func (conn *MockConnection) Reserve(timeout time.Duration) (id uint64, body []byte, err error) {
	time.Sleep(timeout)

	if conn.config != nil {
		assert.Equal(conn.t, conn.config.WaitForConsumerReserve, timeout)
	}

	conn.ReserveFlag--

	return conn.returnReserveId, conn.returnReserveBody, conn.returnReserveError
}

func (conn *MockConnection) Release(id uint64, pri uint32, delay time.Duration) error {
	assert.Equal(conn.t, conn.returnReserveId, id)
	assert.Equal(conn.t, conn.expectReleasePri, pri)
	assert.Equal(conn.t, conn.expectReleaseDelay, delay)

	conn.ReleaseFlag--

	return conn.returnReleaseError
}

func (conn *MockConnection) Delete(id uint64) error {
	assert.Equal(conn.t, conn.returnReserveId, id)

	conn.DeleteFlag--

	return conn.returnDeleteError
}

func (conn *MockConnection) Close() error {
	conn.CloseFlag--

	return conn.returnCloseError
}

func (conn *MockConnection) HandlePayload(task *common.Task) {
	if conn.expectTask != nil {
		assert.Equal(conn.t, conn.expectTask, task)
	}

	conn.HandlePayloadFlag--
}

func NewMockConfiguration() *Configuration {
	config := NewConfiguration()

	config.WaitForConsumerReserve = time.Millisecond * 10
	config.QuitSignalTimeout = time.Second

	return config
}

func setupTest(t *testing.T, conn *MockConnection) func() {
	//log.Logger().Level = logrus.TraceLevel
	// Test setup
	conn.t = t

	c := NewConsumer(conn, conn, conn)

	c.config = conn.config

	if c.config == nil {
		c.config = NewMockConfiguration()
	}

	c.StartConsumer()

	//wait for things to boot up
	time.Sleep(time.Millisecond * 5)

	// Test teardown - return a closure for use by 'defer'
	return func() {
		c.StopConsumer()

		//wait for threads to clean up
		time.Sleep(time.Millisecond)

		util.AssertFlags(t, reflect.ValueOf(conn.Flags))
	}
}

func TestLoadConfiguration(t *testing.T) {
	got, err := LoadConfiguration([]byte("{\"WaitForConsumerReserve\":4, \"QuitSignalTimeout\":5}"))

	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, time.Duration(4), got.WaitForConsumerReserve)
	assert.Equal(t, time.Duration(5), got.QuitSignalTimeout)
}

func TestStartConsumer(t *testing.T) {
	flags := &Flags{
		ReserveFlag: 1, DeleteFlag: 1, CloseFlag: 1,
		StartConsumerFlag: 1, EndConsumerFlag: 1}

	conn := &MockConnection{
		Flags: flags,
	}

	defer setupTest(t, conn)()
}

func TestReserveTask(t *testing.T) {
	flags := &Flags{
		ReserveFlag: 1, HandlePayloadFlag: 1, DeleteFlag: 1, CloseFlag: 1,
		StartConsumerFlag: 1, EndConsumerFlag: 1,
	}

	conn := &MockConnection{
		returnReserveId:   13,
		returnReserveBody: []byte("{\"Name\":\"add\",\"Payload\":\"dGVzdA==\"}"),

		expectTask: &common.Task{Name: "add", Payload: []byte("test")},
		Flags:      flags,
	}

	defer setupTest(t, conn)()
}

func TestQuitSignal(t *testing.T) {
	config := NewMockConfiguration()
	//setting quit signal timeout slightly shorter than final Sleep time
	config.QuitSignalTimeout = time.Millisecond * 20

	flags := &Flags{
		ReserveFlag: 2, DeleteFlag: 2, CloseFlag: 1,
		StartConsumerFlag: 1, EndConsumerFlag: 1, QuitSignalFlag: 1}

	conn := &MockConnection{
		config:            config,
		returnReserveBody: nil,
		Flags:             flags,
	}

	defer setupTest(t, conn)()

	time.Sleep(time.Millisecond * 40)
}

func TestReserveTimeout(t *testing.T) {
	flags := &Flags{
		ReserveFlag: 1, HandlePayloadFlag: 0, DeleteFlag: 0, CloseFlag: 1,
		StartConsumerFlag: 1, EndConsumerFlag: 1,
	}

	conn := &MockConnection{
		returnReserveId:    0,
		returnReserveBody:  nil,
		returnReserveError: beanstalk.ErrTimeout,

		expectTask: &common.Task{Name: "add", Payload: []byte("test")},
		Flags:      flags,
	}

	defer setupTest(t, conn)()
}

func TestTaskErrorHandling(t *testing.T) {
	t.Fail()
}

func TestReleaseTask(t *testing.T) {
	t.Fail()
}
