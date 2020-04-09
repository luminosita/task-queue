package consumer

import (
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/golang/mock/gomock"
	"github.com/mnikita/task-queue/pkg/common"
	cmocks "github.com/mnikita/task-queue/pkg/common/mocks"
	"github.com/mnikita/task-queue/pkg/connector"
	lmocks "github.com/mnikita/task-queue/pkg/consumer/mocks"
	"github.com/mnikita/task-queue/pkg/util"
	"testing"
	"time"
)

type Mock struct {
	t *testing.T

	taskProcessEventHandler common.TaskProcessEventHandler

	*Configuration

	ctrl *gomock.Controller

	handler       *lmocks.MockHandler
	consumerEh    *lmocks.MockEventHandler
	taskPlh       *cmocks.MockTaskPayloadHandler
	taskProcessEh *cmocks.MockTaskProcessEventHandler
}

func NewMockConfiguration() *Configuration {
	config := NewConfiguration()

	config.WaitForConsumerReserve = time.Millisecond * 10
	config.Heartbeat = time.Second

	return config
}

func newMock(t *testing.T) *Mock {
	m := &Mock{}
	m.t = t
	m.ctrl = gomock.NewController(t)

	m.Configuration = NewMockConfiguration()

	m.handler = lmocks.NewMockHandler(m.ctrl)
	m.consumerEh = lmocks.NewMockEventHandler(m.ctrl)
	m.taskPlh = cmocks.NewMockTaskPayloadHandler(m.ctrl)

	return m
}

func setupTest(m *Mock) func() {
	if m == nil {
		panic("Mock not initialized")
	}

	m.consumerEh.EXPECT().OnStartConsume()
	m.consumerEh.EXPECT().OnEndConsume()

	m.handler.EXPECT().Close()

	config := connector.NewConfiguration()
	conn := connector.NewConnector(config)

	c := NewConsumer(m.Configuration, m.handler, conn)
	c.SetEventHandler(m.consumerEh)
	c.SetTaskPayloadHandler(m.taskPlh)

	m.taskProcessEventHandler = conn

	err := c.StartConsumer()

	if err != nil {
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

		c.StopConsumer()

		//wait for threads to clean up
		time.Sleep(time.Millisecond * 10)
	}
}

func TestProcessOneTask(t *testing.T) {
	m := newMock(t)

	reserveTask := &common.Task{Id: 13, Name: "add", Payload: []byte("test")}

	m.handler.EXPECT().Reserve(m.WaitForConsumerReserve).Return(
		uint64(13), []byte("{\"Name\":\"add\",\"Payload\":\"dGVzdA==\"}"), nil)
	m.handler.EXPECT().Delete(uint64(13))
	m.taskPlh.EXPECT().HandlePayload(
		gomock.Eq(reserveTask)).Do(func(task *common.Task) {
		m.taskProcessEventHandler.OnTaskSuccess(task)
	})
	m.handler.EXPECT().Reserve(m.WaitForConsumerReserve).AnyTimes()

	defer setupTest(m)()
}

func TestHeartbeat(t *testing.T) {
	m := newMock(t)
	//setting quit signal timeout slightly shorter than final Sleep time
	m.Heartbeat = time.Millisecond * 50

	m.handler.EXPECT().Reserve(m.WaitForConsumerReserve).Return(uint64(0), nil, nil).AnyTimes()
	m.consumerEh.EXPECT().OnHeartbeat()

	defer setupTest(m)()

	//needs to be slightly bigger than heartbeat
	time.Sleep(time.Millisecond * 40)
}

func TestReserveTimeout(t *testing.T) {
	m := newMock(t)

	m.handler.EXPECT().Reserve(m.WaitForConsumerReserve).Return(uint64(13), nil, beanstalk.ErrTimeout)
	m.consumerEh.EXPECT().OnReserveTimeout()

	defer setupTest(m)()
}

func TestBuryTask(t *testing.T) {
	m := newMock(t)

	buryTask := &common.Task{Id: 13, Name: "add", Payload: []byte("test")}
	threadError := &common.TaskThreadError{Task: buryTask,
		Err: errors.New("test error")}

	m.handler.EXPECT().Reserve(m.WaitForConsumerReserve).Return(
		uint64(13), []byte("{\"Name\":\"add\",\"Payload\":\"dGVzdA==\"}"), nil)
	m.handler.EXPECT().Bury(uint64(13), m.BuryPriority)
	m.taskPlh.EXPECT().HandlePayload(
		gomock.Eq(buryTask)).Do(func(task *common.Task) {
		m.taskProcessEventHandler.OnTaskError(task, threadError)
	})
	m.handler.EXPECT().Reserve(m.WaitForConsumerReserve).AnyTimes()

	defer setupTest(m)()
}

func TestTouchTask(t *testing.T) {
	m := newMock(t)

	deleteTask := &common.Task{Id: 13, Name: "add", Payload: []byte("test")}

	m.handler.EXPECT().Reserve(m.WaitForConsumerReserve).Return(
		uint64(13), []byte("{\"Name\":\"add\",\"Payload\":\"dGVzdA==\"}"), nil)
	m.handler.EXPECT().Touch(uint64(13))
	m.handler.EXPECT().Delete(uint64(13))
	m.taskPlh.EXPECT().HandlePayload(
		gomock.Eq(deleteTask)).Do(func(task *common.Task) {

		m.taskProcessEventHandler.OnTaskHeartbeat(task)
		time.Sleep(time.Millisecond * 20)
		m.taskProcessEventHandler.OnTaskSuccess(task)
	})
	m.handler.EXPECT().Reserve(m.WaitForConsumerReserve).AnyTimes()

	defer setupTest(m)()
}

func TestProcessMultipleTasks(t *testing.T) {
	m := newMock(t)

	//First consumer task
	deleteTask := &common.Task{Id: 13, Name: "ok", Payload: []byte("test")}

	m.handler.EXPECT().Reserve(m.WaitForConsumerReserve).Return(
		uint64(13), []byte("{\"Name\":\"ok\",\"Payload\":\"dGVzdA==\"}"), nil)
	m.handler.EXPECT().Delete(uint64(13))
	m.taskPlh.EXPECT().HandlePayload(
		gomock.Eq(deleteTask)).Do(func(task *common.Task) {
		m.taskProcessEventHandler.OnTaskSuccess(task)
	})

	//Second consumer task
	buryTask := &common.Task{Id: 14, Name: "error", Payload: []byte("test")}

	m.handler.EXPECT().Reserve(m.WaitForConsumerReserve).Return(
		uint64(14), []byte("{\"Name\":\"error\",\"Payload\":\"dGVzdA==\"}"), nil)
	m.handler.EXPECT().Touch(uint64(14))
	m.handler.EXPECT().Bury(uint64(14), m.BuryPriority)
	m.taskPlh.EXPECT().HandlePayload(
		gomock.Eq(buryTask)).Do(func(task *common.Task) {
		m.taskProcessEventHandler.OnTaskHeartbeat(task)
		time.Sleep(time.Millisecond * 20)
		m.taskProcessEventHandler.OnTaskError(task, &common.TaskThreadError{Task: task,
			Err: errors.New("test error")})
	})

	//Third consumer task
	deleteTask = &common.Task{Id: 15, Name: "ok", Payload: []byte("test")}

	m.handler.EXPECT().Reserve(m.WaitForConsumerReserve).Return(
		uint64(15), []byte("{\"Name\":\"ok\",\"Payload\":\"dGVzdA==\"}"), nil)
	m.handler.EXPECT().Delete(uint64(15))
	m.taskPlh.EXPECT().HandlePayload(
		gomock.Eq(deleteTask)).Do(func(task *common.Task) {
		m.taskProcessEventHandler.OnTaskSuccess(task)
	})

	m.handler.EXPECT().Reserve(m.WaitForConsumerReserve).AnyTimes()

	defer setupTest(m)()
}
