package consumer_test

import (
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/mnikita/task-queue/pkg/common"
	cmocks "github.com/mnikita/task-queue/pkg/common/mocks"
	"github.com/mnikita/task-queue/pkg/connector"
	"github.com/mnikita/task-queue/pkg/consumer"
	lmocks "github.com/mnikita/task-queue/pkg/consumer/mocks"
	"github.com/mnikita/task-queue/pkg/util"
	"testing"
	"time"
)

type Mock struct {
	t *testing.T

	taskProcessEventHandler common.TaskProcessEventHandler

	ctrl *gomock.Controller

	cc    *consumer.Configuration
	connC *connector.Configuration

	connectionH   *lmocks.MockConnectionHandler
	consumerEh    *lmocks.MockEventHandler
	taskPlh       *cmocks.MockTaskPayloadHandler
	taskProcessEh *cmocks.MockTaskProcessEventHandler

	consumer  consumer.Handler
	connector connector.Handler
}

func newMock(t *testing.T) *Mock {
	m := &Mock{}
	m.t = t
	m.ctrl = gomock.NewController(t)

	m.connectionH = lmocks.NewMockConnectionHandler(m.ctrl)
	m.consumerEh = lmocks.NewMockEventHandler(m.ctrl)
	m.taskPlh = cmocks.NewMockTaskPayloadHandler(m.ctrl)

	m.cc = consumer.NewConfiguration()
	m.connC = connector.NewConfiguration()

	m.connector = connector.NewConnector(m.connC)
	m.consumer = consumer.NewConsumer(m.cc, m.connector, m.connectionH)

	m.cc.WaitForConsumerReserve = time.Millisecond * 10
	m.cc.Heartbeat = time.Second

	m.consumer.SetEventHandler(m.consumerEh)
	m.consumer.SetTaskPayloadHandler(m.taskPlh)

	m.taskProcessEventHandler = m.connector.(common.TaskProcessEventHandler)

	return m
}

func setupTest(m *Mock) func() {
	if m == nil {
		panic("Mock not initialized")
	}

	if err := m.consumer.Init(); err != nil {
		panic(err)
	}
	if err := m.connector.Init(); err != nil {
		panic(err)
	}

	m.consumerEh.EXPECT().OnStartConsume()
	m.consumerEh.EXPECT().OnEndConsume()

	m.connectionH.EXPECT().Close()

	if err := m.consumer.StartConsumer(); err != nil {
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

		m.consumer.StopConsumer()

		//wait for threads to clean up
		time.Sleep(time.Millisecond * 10)
	}
}

func (m *Mock) getWaitForConsumerReserve() time.Duration {
	return m.cc.WaitForConsumerReserve
}

func (m *Mock) getBuryPriority() uint32 {
	return m.cc.BuryPriority
}

func TestProcessOneTask(t *testing.T) {
	m := newMock(t)

	reserveTask := &common.Task{Id: 13, Name: "add", Payload: []byte("test")}

	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).Return(
		uint64(13), []byte("{\"Name\":\"add\",\"Payload\":\"dGVzdA==\"}"), nil)
	m.connectionH.EXPECT().Delete(uint64(13))
	m.taskPlh.EXPECT().HandlePayload(
		gomock.Eq(reserveTask)).Do(func(task *common.Task) {
		m.taskProcessEventHandler.OnTaskSuccess(task)
	})
	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).AnyTimes()

	defer setupTest(m)()
}

func TestHeartbeat(t *testing.T) {
	m := newMock(t)
	//setting quit signal timeout slightly shorter than final Sleep time
	m.cc.Heartbeat = time.Millisecond * 50

	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).Return(uint64(0), nil, nil).AnyTimes()
	m.consumerEh.EXPECT().OnHeartbeat()

	defer setupTest(m)()

	//needs to be slightly bigger than heartbeat
	time.Sleep(time.Millisecond * 40)
}

func TestReserveTimeout(t *testing.T) {
	m := newMock(t)

	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).Return(uint64(13), nil, consumer.ErrTimeout)
	m.consumerEh.EXPECT().OnReserveTimeout()

	defer setupTest(m)()
}

func TestBuryTask(t *testing.T) {
	m := newMock(t)

	buryTask := &common.Task{Id: 13, Name: "add", Payload: []byte("test")}
	threadError := &common.TaskThreadError{Task: buryTask,
		Err: errors.New("test error")}

	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).Return(
		uint64(13), []byte("{\"Name\":\"add\",\"Payload\":\"dGVzdA==\"}"), nil)
	m.connectionH.EXPECT().Bury(uint64(13), m.getBuryPriority())
	m.taskPlh.EXPECT().HandlePayload(
		gomock.Eq(buryTask)).Do(func(task *common.Task) {
		m.taskProcessEventHandler.OnTaskError(task, threadError)
	})
	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).AnyTimes()

	defer setupTest(m)()
}

func TestTouchTask(t *testing.T) {
	m := newMock(t)

	deleteTask := &common.Task{Id: 13, Name: "add", Payload: []byte("test")}

	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).Return(
		uint64(13), []byte("{\"Name\":\"add\",\"Payload\":\"dGVzdA==\"}"), nil)
	m.connectionH.EXPECT().Touch(uint64(13))
	m.connectionH.EXPECT().Delete(uint64(13))
	m.taskPlh.EXPECT().HandlePayload(
		gomock.Eq(deleteTask)).Do(func(task *common.Task) {

		m.taskProcessEventHandler.OnTaskHeartbeat(task)
		time.Sleep(time.Millisecond * 20)
		m.taskProcessEventHandler.OnTaskSuccess(task)
	})
	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).AnyTimes()

	defer setupTest(m)()
}

func TestProcessMultipleTasks(t *testing.T) {
	m := newMock(t)

	//First consumer task
	deleteTask := &common.Task{Id: 13, Name: "ok", Payload: []byte("test")}

	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).Return(
		uint64(13), []byte("{\"Name\":\"ok\",\"Payload\":\"dGVzdA==\"}"), nil)
	m.connectionH.EXPECT().Delete(uint64(13))
	m.taskPlh.EXPECT().HandlePayload(
		gomock.Eq(deleteTask)).Do(func(task *common.Task) {
		m.taskProcessEventHandler.OnTaskSuccess(task)
	})

	//Second consumer task
	buryTask := &common.Task{Id: 14, Name: "error", Payload: []byte("test")}

	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).Return(
		uint64(14), []byte("{\"Name\":\"error\",\"Payload\":\"dGVzdA==\"}"), nil)
	m.connectionH.EXPECT().Touch(uint64(14))
	m.connectionH.EXPECT().Bury(uint64(14), m.getBuryPriority())
	m.taskPlh.EXPECT().HandlePayload(
		gomock.Eq(buryTask)).Do(func(task *common.Task) {
		m.taskProcessEventHandler.OnTaskHeartbeat(task)
		time.Sleep(time.Millisecond * 20)
		m.taskProcessEventHandler.OnTaskError(task, &common.TaskThreadError{Task: task,
			Err: errors.New("test error")})
	})

	//Third consumer task
	deleteTask = &common.Task{Id: 15, Name: "ok", Payload: []byte("test")}

	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).Return(
		uint64(15), []byte("{\"Name\":\"ok\",\"Payload\":\"dGVzdA==\"}"), nil)
	m.connectionH.EXPECT().Delete(uint64(15))
	m.taskPlh.EXPECT().HandlePayload(
		gomock.Eq(deleteTask)).Do(func(task *common.Task) {
		m.taskProcessEventHandler.OnTaskSuccess(task)
	})

	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).AnyTimes()

	defer setupTest(m)()
}

func TestProcessEmptyPayload(t *testing.T) {
	m := newMock(t)

	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).Return(
		uint64(13), []byte("{\"Name\":\"laza\",\"Payload\":{}}"), nil)
	m.connectionH.EXPECT().Bury(uint64(13), m.getBuryPriority())
	m.connectionH.EXPECT().Reserve(m.getWaitForConsumerReserve()).AnyTimes()

	defer setupTest(m)()
}
