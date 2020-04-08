package worker

import (
	"errors"
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/mnikita/task-queue/pkg/connector"
	"github.com/mnikita/task-queue/pkg/util"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"testing"
	"time"
)

type Mock struct {
	t *testing.T

	*Configuration
	*Worker
	common.TaskPayloadHandler
}

type MockEventHandler struct {
	t *testing.T

	expectedThreadHeartbeatThreadId int

	expectedProcessTask  *common.Task
	expectedProcessError bool

	expectedPreTask           *common.Task
	expectedPostTask          *common.Task
	expectedAcceptTaskTask    *common.Task
	expectedAcceptTimeoutTask *common.Task

	*Flags
}

type Flags struct {
	WorkerStart          int
	WorkerEnd            int
	ThreadHeartbeat      int
	PreTask              int
	PostTag              int
	QueuedTask           int
	QueueTimeout         int
	TaskProcessError     int
	TaskProcessSuccess   int
	TaskProcessHeartbeat int
}

type TestTask struct {
	t *testing.T

	eventHandler common.TaskProcessEventHandler
}

func (task *TestTask) SetTaskProcessEventHandler(eventHandler common.TaskProcessEventHandler) {
	task.eventHandler = eventHandler
}

func (task *TestTask) Handle() error {
	time.Sleep(time.Millisecond * 20)

	return nil
}

type LongTask struct {
	t *testing.T

	eventHandler common.TaskProcessEventHandler
}

func (task *LongTask) SetTaskProcessEventHandler(eventHandler common.TaskProcessEventHandler) {
	task.eventHandler = eventHandler
}

func (task *LongTask) Handle() error {
	time.Sleep(time.Millisecond * 100)

	return nil
}

var errorTaskError = errors.New("ErrorTask test error")

type ErrorTask struct {
	t *testing.T

	eventHandler common.TaskProcessEventHandler
}

func (task *ErrorTask) SetTaskProcessEventHandler(eventHandler common.TaskProcessEventHandler) {
	task.eventHandler = eventHandler
}

func (task *ErrorTask) Handle() error {
	time.Sleep(time.Millisecond * 10)

	return errorTaskError
}

func (h *MockEventHandler) OnStartWorker() {
	h.WorkerStart--
}

func (h *MockEventHandler) OnEndWorker() {
	h.WorkerEnd--
}

func (h *MockEventHandler) OnThreadHeartbeat(threadId int) {
	if h.expectedThreadHeartbeatThreadId != 0 {
		assert.Equal(h.t, h.expectedThreadHeartbeatThreadId, threadId)
	}

	h.ThreadHeartbeat--
}

func (h *MockEventHandler) OnTaskProcessEvent(event *common.TaskProcessEvent) {
	if h.expectedProcessError {
		assert.NotNil(h.t, event.Err)
	}

	if h.expectedProcessTask != nil {
		assert.Equal(h.t, h.expectedProcessTask, event.Task)
	}

	switch event.EventId {
	case common.Success:
		h.TaskProcessSuccess--
	case common.Error:
		h.TaskProcessError--
	case common.Heartbeat:
		h.TaskProcessHeartbeat--
	}
}

//TODO: Implement
func (h *MockEventHandler) OnResult(a ...interface{}) error {
	panic("implement me")
}

func (h *MockEventHandler) OnPreTask(task *common.Task) {
	if h.expectedPreTask != nil {
		assert.Equal(h.t, h.expectedPreTask, task)
	}

	h.PreTask--
}

func (h *MockEventHandler) OnPostTask(task *common.Task) {
	if h.expectedPostTask != nil {
		assert.Equal(h.t, h.expectedPostTask, task)
	}

	h.PostTag--
}

func (h *MockEventHandler) OnTaskQueued(task *common.Task) {
	if h.expectedAcceptTaskTask != nil {
		assert.Equal(h.t, h.expectedAcceptTaskTask, task)
	}

	h.QueuedTask--
}

func (h *MockEventHandler) OnTaskAcceptTimeout(task *common.Task) {
	if h.expectedAcceptTimeoutTask != nil {
		assert.Equal(h.t, h.expectedAcceptTimeoutTask, task)
	}

	h.QueueTimeout--
}

func TestMain(m *testing.M) {
	//log.Logger().Level = logrus.TraceLevel

	common.RegisterTask("test", func() common.TaskHandler {
		return &TestTask{}
	})

	common.RegisterTask("long", func() common.TaskHandler {
		return &LongTask{}
	})

	common.RegisterTask("error", func() common.TaskHandler {
		return &ErrorTask{}
	})

	os.Exit(m.Run())
}

func newMock() *Mock {
	mock := &Mock{}
	mock.Configuration = NewConfiguration()
	mock.WaitTaskThreadsToClose = time.Second * 2

	return mock
}

func setupTest(t *testing.T, mock *Mock, eh *MockEventHandler) func() {
	// Test setup
	if mock == nil {
		mock = newMock()
	}

	mock.t = t

	if eh == nil {
		eh = &MockEventHandler{Flags: &Flags{WorkerStart: 1, WorkerEnd: 1}}
	}

	eh.t = t

	config := connector.NewConfiguration()
	config.WaitToAcceptConsumerTask = mock.Configuration.WaitToAcceptConsumerTask

	conn := connector.NewConnector(config)

	mock.TaskPayloadHandler = conn

	conn.SetEventHandler(eh)

	worker := NewWorker(mock.Configuration, conn)
	worker.SetEventHandler(eh)
	worker.SetTaskEventHandler(eh)

	mock.Worker = worker

	worker.StartWorker()

	//wait for things to boot up
	time.Sleep(time.Millisecond * 5)

	// Test teardown - return a closure for use by 'defer'
	return func() {
		worker.StopWorker()

		//wait for threads to clean up
		time.Sleep(time.Millisecond * 1)

		if eh.Flags != nil {
			util.AssertFlags(t, reflect.ValueOf(eh.Flags))
		}
	}
}

func TestStartServer(t *testing.T) {
	defer setupTest(t, nil, nil)()
}

func TestHandleTask(t *testing.T) {
	flags := &Flags{WorkerStart: 1, WorkerEnd: 1, PreTask: 1, PostTag: 1, TaskProcessSuccess: 1, QueuedTask: 1}
	eh := &MockEventHandler{Flags: flags}

	mock := newMock()

	defer setupTest(t, mock, eh)()

	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})
	//TODO: Validate proper expectedProcessTask

}

func TestHandleMultipleTask(t *testing.T) {
	flags := &Flags{WorkerStart: 1, WorkerEnd: 1, PreTask: 10, PostTag: 10, TaskProcessSuccess: 10, QueuedTask: 10}
	eh := &MockEventHandler{Flags: flags}

	mock := newMock()

	defer setupTest(t, mock, eh)()

	for i := 0; i < 5; i++ {
		mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})
		mock.HandlePayload(&common.Task{Name: "long", Payload: []byte("{}")})
	}

	time.Sleep(time.Second)
}

func TestKillServer(t *testing.T) {
	flags := &Flags{WorkerStart: 1, WorkerEnd: 1, PreTask: 1, QueuedTask: 1}
	eh := &MockEventHandler{Flags: flags}

	mock := newMock()
	mock.WaitTaskThreadsToClose = time.Millisecond * 50

	defer setupTest(t, mock, eh)()

	mock.HandlePayload(&common.Task{Name: "long", Payload: []byte("{}")})
}

func TestHandlePayloadTimeout(t *testing.T) {
	flags := &Flags{WorkerStart: 1, WorkerEnd: 1, PreTask: 4, PostTag: 4, QueuedTask: 4, TaskProcessSuccess: 4,
		QueueTimeout: 1}

	eh := &MockEventHandler{Flags: flags}

	mock := newMock()
	mock.Concurrency = 2
	mock.WaitToAcceptConsumerTask = time.Millisecond * 10

	defer setupTest(t, mock, eh)()

	//first task should get processed
	mock.HandlePayload(&common.Task{Name: "long", Payload: []byte("{}")})

	//second will be queued
	mock.HandlePayload(&common.Task{Name: "long", Payload: []byte("{}")})
	//third should hang and issue timeout
	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})
	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})
	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})

	time.Sleep(time.Second)
}

func TestHeartbeat(t *testing.T) {
	flags := &Flags{WorkerStart: 1, WorkerEnd: 1, PreTask: 2, PostTag: 2, QueuedTask: 2, TaskProcessSuccess: 2,
		ThreadHeartbeat: 1}

	eh := &MockEventHandler{Flags: flags}

	mock := newMock()
	mock.Concurrency = 1
	mock.Heartbeat = time.Millisecond * 15

	defer setupTest(t, mock, eh)()

	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})

	//setting time between sending two tasks to allow for heartbeat
	time.Sleep(time.Millisecond * 40)

	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})

	time.Sleep(time.Millisecond * 20)
}

func TestTaskThreadError(t *testing.T) {
	flags := &Flags{WorkerStart: 1, WorkerEnd: 1, PreTask: 3, PostTag: 2, QueuedTask: 3, TaskProcessSuccess: 2,
		TaskProcessError: 1}

	errorTask := &common.Task{Name: "error", Payload: []byte("{}")}

	//TODO: Test with multiple tasks test data
	eh := &MockEventHandler{Flags: flags}
	//	, expectedProcessError: true,
	//		expectedProcessTask: errorTask}

	mock := newMock()

	defer setupTest(t, mock, eh)()

	mock.HandlePayload(&common.Task{Name: "long", Payload: []byte("{}")})
	mock.HandlePayload(errorTask)
	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})

	time.Sleep(time.Millisecond * 200)
}

func TestGetUnregisteredTask(t *testing.T) {
	flags := &Flags{WorkerStart: 1, WorkerEnd: 1, QueuedTask: 1, TaskProcessError: 1}
	eh := &MockEventHandler{Flags: flags}

	mock := newMock()

	defer setupTest(t, mock, eh)()

	mock.HandlePayload(&common.Task{Name: "unknown", Payload: []byte("{}")})
}

//TODO: Implement
func TestTaskResult(t *testing.T) {
	t.FailNow()
}
