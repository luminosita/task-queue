package worker_test

import (
	"github.com/golang/mock/gomock"
	"github.com/mnikita/task-queue/pkg/common"
	cmocks "github.com/mnikita/task-queue/pkg/common/mocks"
	"github.com/mnikita/task-queue/pkg/connector"
	"github.com/mnikita/task-queue/pkg/log"
	"github.com/mnikita/task-queue/pkg/util"
	"github.com/mnikita/task-queue/pkg/worker"
	wmocks "github.com/mnikita/task-queue/pkg/worker/mocks"
	"os"
	"testing"
	"time"
)

type Mock struct {
	t *testing.T

	common.TaskPayloadHandler

	ctrl *gomock.Controller

	workerEh      *wmocks.MockEventHandler
	taskQueueEh   *cmocks.MockTaskQueueEventHandler
	taskProcessEh *cmocks.MockTaskProcessEventHandler

	wc *worker.Configuration
	cc *connector.Configuration

	worker    worker.Handler
	connector connector.Handler
}

func TestMain(m *testing.M) {
	//	log.Logger().Level = logrus.TraceLevel

	wmocks.RegisterTasks()

	os.Exit(m.Run())
}

func newMock(t *testing.T) *Mock {
	m := &Mock{}
	m.t = t
	m.ctrl = gomock.NewController(t)

	m.workerEh = wmocks.NewMockEventHandler(m.ctrl)
	m.taskQueueEh = cmocks.NewMockTaskQueueEventHandler(m.ctrl)
	m.taskProcessEh = cmocks.NewMockTaskProcessEventHandler(m.ctrl)

	m.wc = worker.NewConfiguration()
	m.cc = connector.NewConfiguration()

	m.connector = connector.NewConnector(m.cc)
	m.worker = worker.NewWorker(m.wc, m.connector)

	m.wc.WaitTaskThreadsToClose = time.Second * 2

	m.connector.SetEventHandler(m.taskQueueEh)

	m.worker.SetEventHandler(m.workerEh)
	m.worker.SetTaskEventHandler(m.taskProcessEh)

	m.TaskPayloadHandler = m.connector.(common.TaskPayloadHandler)

	return m
}

func setupTest(m *Mock) func() {
	if m == nil {
		panic("Mock not initialized")
	}

	if err := m.worker.Init(); err != nil {
		panic(err)
	}
	if err := m.connector.Init(); err != nil {
		panic(err)
	}

	m.workerEh.EXPECT().OnStartWorker()
	m.workerEh.EXPECT().OnEndWorker()

	m.worker.StartWorker()

	//wait for things to boot up
	time.Sleep(time.Millisecond * 5)

	// Test teardown - return a closure for use by 'defer'
	return func() {
		defer m.ctrl.Finish()
		defer util.AssertPanic(m.t)

		m.worker.StopWorker()

		//wait for threads to clean up
		time.Sleep(time.Millisecond * 1)
	}
}

func TestStartServer(t *testing.T) {
	defer setupTest(newMock(t))()
}

func TestHandleTaskWithMock(t *testing.T) {
	m := newMock(t)
	defer setupTest(m)()

	shortTask := &common.Task{Name: wmocks.Tasks[wmocks.Short], Payload: []byte("{}")}

	m.workerEh.EXPECT().OnPreTask(shortTask)
	m.workerEh.EXPECT().OnPostTask(shortTask)

	m.taskQueueEh.EXPECT().OnTaskQueued(shortTask)

	m.taskProcessEh.EXPECT().OnTaskSuccess(shortTask)

	m.HandlePayload(shortTask)
}

func TestHandleMultipleTask(t *testing.T) {
	m := newMock(t)
	defer setupTest(m)()

	shortTask := &common.Task{Name: wmocks.Tasks[wmocks.Short], Payload: []byte("{}")}
	longTask := &common.Task{Name: wmocks.Tasks[wmocks.Long], Payload: []byte("{}")}

	m.workerEh.EXPECT().OnPreTask(shortTask).Times(5)
	m.workerEh.EXPECT().OnPreTask(longTask).Times(5)
	m.workerEh.EXPECT().OnPostTask(shortTask).Times(5)
	m.workerEh.EXPECT().OnPostTask(longTask).Times(5)

	m.taskQueueEh.EXPECT().OnTaskQueued(shortTask).Times(5)
	m.taskQueueEh.EXPECT().OnTaskQueued(longTask).Times(5)

	m.taskProcessEh.EXPECT().OnTaskSuccess(shortTask).Times(5)
	m.taskProcessEh.EXPECT().OnTaskSuccess(longTask).Times(5)

	for i := 0; i < 5; i++ {
		m.HandlePayload(shortTask)
		m.HandlePayload(longTask)
	}

	time.Sleep(time.Second)
}

func TestHandlePayloadTimeout(t *testing.T) {
	m := newMock(t)
	m.wc.Concurrency = 2
	m.cc.WaitToAcceptConsumerTask = time.Millisecond * 10

	defer setupTest(m)()

	shortTask := &common.Task{Name: wmocks.Tasks[wmocks.Short], Payload: []byte("{}")}
	longTask := &common.Task{Name: wmocks.Tasks[wmocks.Long], Payload: []byte("{}")}

	m.workerEh.EXPECT().OnPreTask(longTask).Times(2)
	m.workerEh.EXPECT().OnPreTask(shortTask).Times(2)
	m.workerEh.EXPECT().OnPostTask(longTask).Times(2)
	m.workerEh.EXPECT().OnPostTask(shortTask).Times(2)

	m.taskQueueEh.EXPECT().OnTaskQueued(longTask).Times(2)
	m.taskQueueEh.EXPECT().OnTaskQueued(shortTask).Times(2)
	m.taskQueueEh.EXPECT().OnTaskAcceptTimeout(shortTask).Times(1)

	m.taskProcessEh.EXPECT().OnTaskSuccess(longTask).Times(2)
	m.taskProcessEh.EXPECT().OnTaskSuccess(shortTask).Times(2)

	//first task should get processed
	m.HandlePayload(longTask)

	//second will be queued
	m.HandlePayload(longTask)
	//third should hang and issue timeout
	m.HandlePayload(shortTask)
	m.HandlePayload(shortTask)
	m.HandlePayload(shortTask)

	time.Sleep(time.Second)
}

func TestHeartbeat(t *testing.T) {
	m := newMock(t)
	m.wc.Concurrency = 1
	m.wc.Heartbeat = time.Millisecond * 15

	defer setupTest(m)()

	shortTask := &common.Task{Name: wmocks.Tasks[wmocks.Short], Payload: []byte("{}")}

	m.workerEh.EXPECT().OnPreTask(shortTask).Times(2)
	m.workerEh.EXPECT().OnPostTask(shortTask).Times(2)
	//cannot be sure which thread will timeout
	m.workerEh.EXPECT().OnThreadHeartbeat(gomock.Any()).Times(1)

	m.taskQueueEh.EXPECT().OnTaskQueued(shortTask).Times(2)

	m.taskProcessEh.EXPECT().OnTaskSuccess(shortTask).Times(2)

	m.HandlePayload(shortTask)

	//setting time between sending two tasks to allow for heartbeat
	time.Sleep(time.Millisecond * 40)

	m.HandlePayload(shortTask)

	time.Sleep(time.Millisecond * 20)
}

func TestTaskThreadError(t *testing.T) {
	m := newMock(t)

	defer setupTest(m)()

	shortTask := &common.Task{Name: wmocks.Tasks[wmocks.Short], Payload: []byte("{}")}
	longTask := &common.Task{Name: wmocks.Tasks[wmocks.Long], Payload: []byte("{}")}
	errorTask := &common.Task{Name: wmocks.Tasks[wmocks.Error], Payload: []byte("{}")}

	m.workerEh.EXPECT().OnPreTask(shortTask).Times(1)
	m.workerEh.EXPECT().OnPreTask(longTask).Times(1)
	m.workerEh.EXPECT().OnPreTask(errorTask).Times(1)
	m.workerEh.EXPECT().OnPostTask(shortTask).Times(1)
	m.workerEh.EXPECT().OnPostTask(longTask).Times(1)

	m.taskQueueEh.EXPECT().OnTaskQueued(shortTask).Times(1)
	m.taskQueueEh.EXPECT().OnTaskQueued(longTask).Times(1)
	m.taskQueueEh.EXPECT().OnTaskQueued(errorTask).Times(1)

	m.taskProcessEh.EXPECT().OnTaskSuccess(shortTask).Times(1)
	m.taskProcessEh.EXPECT().OnTaskSuccess(longTask).Times(1)
	m.taskProcessEh.EXPECT().OnTaskError(errorTask, util.ErrEq(
		log.TaskThreadError(errorTask.Name, wmocks.ErrorTaskErr))).Times(1)

	m.HandlePayload(longTask)
	m.HandlePayload(errorTask)
	m.HandlePayload(shortTask)

	time.Sleep(time.Millisecond * 200)
}

func TestGetUnregisteredTask(t *testing.T) {
	m := newMock(t)

	defer setupTest(m)()

	unknownTask := &common.Task{Name: "unknown", Payload: []byte("{}")}

	m.taskQueueEh.EXPECT().OnTaskQueued(unknownTask)

	m.taskProcessEh.EXPECT().OnTaskError(unknownTask, util.ErrEq(
		log.TaskThreadError(unknownTask.Name, log.RegisteredTaskHandlerError(unknownTask.Name))))

	m.HandlePayload(unknownTask)
}

func TestTaskResult(t *testing.T) {
	m := newMock(t)
	defer setupTest(m)()

	shortTask := &common.Task{Name: wmocks.Tasks[wmocks.ShortResult], Payload: []byte("{}")}

	m.workerEh.EXPECT().OnPreTask(shortTask)
	m.workerEh.EXPECT().OnPostTask(shortTask)

	m.taskQueueEh.EXPECT().OnTaskQueued(shortTask)

	m.taskProcessEh.EXPECT().OnTaskSuccess(shortTask)
	m.taskProcessEh.EXPECT().OnTaskResult(shortTask, gomock.Eq([]interface{}{wmocks.ShortTaskResult}))

	m.HandlePayload(shortTask)
}

func TestTaskHeartbeat(t *testing.T) {
	m := newMock(t)
	defer setupTest(m)()

	heartbeatTask := &common.Task{Name: wmocks.Tasks[wmocks.Heartbeat], Payload: []byte("{}")}

	m.workerEh.EXPECT().OnPreTask(heartbeatTask)
	m.workerEh.EXPECT().OnPostTask(heartbeatTask)

	m.taskQueueEh.EXPECT().OnTaskQueued(heartbeatTask)

	m.taskProcessEh.EXPECT().OnTaskSuccess(heartbeatTask)
	m.taskProcessEh.EXPECT().OnTaskHeartbeat(heartbeatTask)

	m.HandlePayload(heartbeatTask)
}
