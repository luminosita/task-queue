package consumer

import (
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/mnikita/task-queue/pkg/connector"
	"github.com/mnikita/task-queue/pkg/util"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
	"time"
)

type Mock struct {
	t *testing.T

	taskProcessEventHandler common.TaskProcessEventHandler

	*Configuration

	*Flags

	td              *TestData
	testDataCounter int
	testData        []*TestData
}

type TestData struct {
	returnReserveId    uint64
	returnReserveBody  []byte
	returnReserveError error

	expectReleaseId    uint64
	expectReleasePri   uint32
	expectReleaseDelay time.Duration
	returnReleaseError error

	expectTouchId    uint64
	returnTouchError error

	expectBuryId    uint64
	expectBuryPri   uint32
	returnBuryError error

	expectDeleteId    uint64
	returnDeleteError error

	returnCloseError error

	expectTask *common.Task

	handlePayloadSleep           time.Duration
	returnHandlePayloadSuccess   bool
	returnHandlePayloadHeartbeat bool
	returnHandlePayloadError     *common.TaskThreadError
}

type Flags struct {
	StartConsumerFlag int
	EndConsumerFlag   int
	HeartbeatFlag     int

	ReserveFlag        int
	ReleaseFlag        int
	TouchFlag          int
	BuryFlag           int
	DeleteFlag         int
	CloseFlag          int
	HandlePayloadFlag  int
	ReserveTimeoutFlag int
}

func (conn *Mock) OnReserveTimeout() {
	conn.ReserveTimeoutFlag--
}

func (conn *Mock) OnStartConsume() {
	conn.StartConsumerFlag--
}

func (conn *Mock) OnEndConsume() {
	conn.EndConsumerFlag--
}

func (conn *Mock) OnHeartbeat() {
	conn.HeartbeatFlag--
}

func (conn *Mock) Reserve(timeout time.Duration) (id uint64, body []byte, err error) {
	if conn.td.returnReserveId == 0 {
		return 0, nil, nil
	}

	time.Sleep(timeout)

	id, conn.td.returnReserveId = conn.td.returnReserveId, 0
	body, conn.td.returnReserveBody = conn.td.returnReserveBody, nil
	err, conn.td.returnReserveError = conn.td.returnReserveError, nil

	if conn.Configuration != nil {
		assert.Equal(conn.t, conn.WaitForConsumerReserve, timeout)
	}

	conn.ReserveFlag--

	return
}

func (conn *Mock) Release(id uint64, pri uint32, delay time.Duration) (err error) {
	assert.Equal(conn.t, conn.td.expectReleaseId, id)
	assert.Equal(conn.t, conn.td.expectReleasePri, pri)
	assert.Equal(conn.t, conn.td.expectReleaseDelay, delay)

	err, conn.td.returnReleaseError = conn.td.returnReleaseError, nil

	conn.ReleaseFlag--

	return
}

func (conn *Mock) Bury(id uint64, pri uint32) (err error) {
	assert.Equal(conn.t, conn.td.expectBuryId, id)
	assert.Equal(conn.t, conn.td.expectBuryPri, pri)

	err, conn.td.returnReleaseError = conn.td.returnReleaseError, nil

	conn.BuryFlag--

	return
}

func (conn *Mock) Touch(id uint64) (err error) {
	assert.Equal(conn.t, conn.td.expectTouchId, id)

	err, conn.td.returnTouchError = conn.td.returnTouchError, nil

	conn.TouchFlag--

	return
}

func (conn *Mock) Delete(id uint64) (err error) {
	assert.Equal(conn.t, conn.td.expectDeleteId, id)

	err, conn.td.returnDeleteError = conn.td.returnDeleteError, nil

	conn.DeleteFlag--

	return
}

func (conn *Mock) Close() (err error) {
	err, conn.td.returnCloseError = conn.td.returnCloseError, nil

	conn.CloseFlag--

	return
}

func (conn *Mock) HandlePayload(task *common.Task) {
	if conn.td.expectTask != nil {
		assert.Equal(conn.t, conn.td.expectTask, task)
	}

	if conn.td.handlePayloadSleep != 0 {
		time.Sleep(conn.td.handlePayloadSleep)
	}

	if conn.td.returnHandlePayloadError != nil {
		conn.taskProcessEventHandler.OnTaskProcessEvent(&common.TaskProcessEvent{
			EventId: common.Error, Task: task, Err: conn.td.returnHandlePayloadError})

		conn.td.returnHandlePayloadError = nil
	}

	if conn.td.returnHandlePayloadSuccess {
		conn.taskProcessEventHandler.OnTaskProcessEvent(&common.TaskProcessEvent{
			EventId: common.Success, Task: task})

		conn.td.returnHandlePayloadSuccess = false
	}

	if conn.td.returnHandlePayloadHeartbeat {
		conn.taskProcessEventHandler.OnTaskProcessEvent(&common.TaskProcessEvent{
			EventId: common.Heartbeat, Task: task})

		conn.td.returnHandlePayloadHeartbeat = false
	}

	conn.HandlePayloadFlag--
}

func NewMockConfiguration() *Configuration {
	config := NewConfiguration()

	config.WaitForConsumerReserve = time.Millisecond * 10
	config.Heartbeat = time.Second

	return config
}

func newMock() *Mock {
	mock := &Mock{}
	mock.Configuration = NewMockConfiguration()

	return mock
}

func applyMock(mock *Mock) *Mock {
	mock.Configuration = NewMockConfiguration()

	return mock
}

func setupTest(t *testing.T, mock *Mock) func() {
	//log.Logger().Level = logrus.TraceLevel
	// Test setup
	mock.t = t

	if mock == nil {
		mock = newMock()
	}

	conn := connector.NewConnector(nil)

	c := NewConsumer(mock.Configuration, mock, conn)

	c.eventHandler = mock
	c.taskPayloadHandler = mock

	mock.taskProcessEventHandler = conn

	mock.td = mock.testData[0]

	err := c.StartConsumer()

	if err != nil {
		panic(err)
	}

	//wait for things to boot up
	time.Sleep(time.Millisecond * 5)

	// Test teardown - return a closure for use by 'defer'
	return func() {
		defer util.AssertPanic(t)

		c.StopConsumer()

		//wait for threads to clean up
		time.Sleep(time.Millisecond)

		util.AssertFlags(t, reflect.ValueOf(mock.Flags))
	}
}

func TestStartConsumer(t *testing.T) {
	flags := &Flags{
		CloseFlag:         1,
		StartConsumerFlag: 1, EndConsumerFlag: 1}

	conn := applyMock(&Mock{
		testData: []*TestData{{}},
		Flags:    flags,
	})

	defer setupTest(t, conn)()
}

func TestProcessOneTask(t *testing.T) {
	flags := &Flags{
		ReserveFlag: 1, HandlePayloadFlag: 1, DeleteFlag: 1, CloseFlag: 1,
		StartConsumerFlag: 1, EndConsumerFlag: 1,
	}

	conn := applyMock(&Mock{
		testData: []*TestData{{
			returnReserveId:   13,
			returnReserveBody: []byte("{\"Name\":\"add\",\"Payload\":\"dGVzdA==\"}"),

			expectTask: &common.Task{Id: 13, Name: "add", Payload: []byte("test")},

			expectDeleteId:             13,
			returnHandlePayloadSuccess: true,
		}},
		Flags: flags,
	})

	defer setupTest(t, conn)()

	//wait for task process event
	time.Sleep(time.Millisecond * 10)
}

func TestHeartbeat(t *testing.T) {
	config := NewMockConfiguration()
	//setting quit signal timeout slightly shorter than final Sleep time
	config.Heartbeat = time.Millisecond * 15

	flags := &Flags{
		CloseFlag:         1,
		StartConsumerFlag: 1, EndConsumerFlag: 1, HeartbeatFlag: 1}

	conn := applyMock(&Mock{
		Configuration: config,
		testData: []*TestData{{
			returnReserveId: 0,
		}},
		Flags: flags,
	})

	defer setupTest(t, conn)()

	//needs to be slightly bigger than heartbeat
	time.Sleep(time.Millisecond * 40)
}

func TestReserveTimeout(t *testing.T) {
	flags := &Flags{
		ReserveFlag: 1, CloseFlag: 1, StartConsumerFlag: 1, EndConsumerFlag: 1,
		ReserveTimeoutFlag: 1,
	}

	conn := applyMock(&Mock{
		testData: []*TestData{{
			returnReserveId:    13,
			returnReserveError: beanstalk.ErrTimeout,
		}},

		Flags: flags,
	})

	defer setupTest(t, conn)()
}

func TestBuryTask(t *testing.T) {
	flags := &Flags{
		ReserveFlag: 1, HandlePayloadFlag: 1, BuryFlag: 1, CloseFlag: 1,
		StartConsumerFlag: 1, EndConsumerFlag: 1,
	}

	buryTask := &common.Task{Id: 13, Name: "add", Payload: []byte("test")}

	conn := applyMock(&Mock{
		testData: []*TestData{{
			returnReserveId:   13,
			returnReserveBody: []byte("{\"Name\":\"add\",\"Payload\":\"dGVzdA==\"}"),

			expectBuryId: 13,

			expectTask: buryTask,
			returnHandlePayloadError: &common.TaskThreadError{ThreadId: 1, Task: buryTask,
				Err: errors.New("test error")},
		}},

		Flags: flags,
	})

	defer setupTest(t, conn)()

	//wait for task process event
	time.Sleep(time.Millisecond * 10)
}

func TestTouchTask(t *testing.T) {
	t.FailNow()
}

func TestProcessMultipleTasks(t *testing.T) {
	//TODO: Kod za promenu test data mora da ide na svaki metod zasebno
	//TODO: Funkcije (Reserve, Delete, Release ...) imaju zasebne lifecycle-e zbog confirmation threada
	//special treatment for test data
	//conn.testDataCounter++

	//if conn.testDataCounter < cap(conn.testData) {
	//	conn.td = conn.testData[conn.testDataCounter]
	//}

	t.FailNow()

	//flags := &Flags{
	//	ReserveFlag: 3, HandlePayloadFlag: 3, DeleteFlag: 2, ReleaseFlag: 1, CloseFlag: 1,
	//	StartConsumerFlag: 1, EndConsumerFlag: 1,
	//}
	//
	//releaseTask := &common.Task{Id: 14, Name: "error", Payload: []byte("test")}
	//
	//conn := applyMock(&Mock{
	//	testData: []*TestData{{
	//		returnReserveId:   13,
	//		returnReserveBody: []byte("{\"Name\":\"ok\",\"Payload\":\"dGVzdA==\"}"),
	//
	//		expectTask: &common.Task{Id: 13, Name: "ok", Payload: []byte("test")},
	//
	//		expectDeleteId:             13,
	//		returnHandlePayloadSuccess: true,
	//	},{
	//		returnReserveId:   14,
	//		returnReserveBody: []byte("{\"Name\":\"error\",\"Payload\":\"dGVzdA==\"}"),
	//
	//
	//		expectReleaseId:             14,
	//		expectTask: releaseTask,
	//		returnHandlePayloadError: &common.TaskThreadError{ThreadId: 1, Task: releaseTask,
	//			Err: errors.New("test error")},
	//	},{
	//		returnReserveId:   15,
	//		returnReserveBody: []byte("{\"Name\":\"ok\",\"Payload\":\"dGVzdA==\"}"),
	//
	//		expectTask: &common.Task{Id: 15, Name: "ok", Payload: []byte("test")},
	//
	//		expectDeleteId:             15,
	//		returnHandlePayloadSuccess: true,
	//	}},
	//
	//	Flags: flags,
	//})
	//
	//defer setupTest(t, conn)()
	//
	////wait for task process event
	//time.Sleep(time.Millisecond * 10)
}
