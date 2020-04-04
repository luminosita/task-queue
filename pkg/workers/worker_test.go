package workers

import (
	"errors"
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/mnikita/task-queue/pkg/log"
	"github.com/mnikita/task-queue/pkg/util"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"runtime"
	"testing"
	"time"
)

type Mock struct {
	t *testing.T

	*Configuration
	*Worker
}

type MockEventHandler struct {
	t *testing.T

	expectedThreadHeartbeatThreadId int

	expectedErrorTask *common.Task
	expectedErrorErr  error

	expectedPreTask           *common.Task
	expectedPostTask          *common.Task
	expectedAcceptTaskTask    *common.Task
	expectedAcceptTimeoutTask *common.Task

	*Flags
}

type Flags struct {
	ThreadHeartbeatFlag int
	ErrorFlag           int
	PreTaskFlag         int
	PostTagFlag         int
	QueuedTaskFlag      int
	QueueTimeoutFlag    int
}

type TestTask struct {
	t *testing.T

	callbackFlag     int
	expectedCallback common.TaskHandlerCallback
}

func (task *TestTask) SetCallback(callback common.TaskHandlerCallback) {
	if task.expectedCallback != nil {
		assert.Equal(task.t, task.expectedCallback, callback)
	}

	task.callbackFlag--
	log.Logger().Tracef("Test OnPreTask: %d", task.callbackFlag)

	assert.True(task.t, task.callbackFlag >= 0)
}

func (task *TestTask) Handle() error {
	time.Sleep(time.Millisecond * 20)

	return nil
}

type LongTask struct {
	*TestTask
}

func (task *LongTask) Handle() error {
	time.Sleep(time.Millisecond * 100)

	return nil
}

type ErrorTask struct {
	*TestTask
}

func (task *ErrorTask) Handle() error {
	time.Sleep(time.Millisecond * 10)

	return errors.New("ErrorTask test error")
}

func (h *MockEventHandler) OnThreadHeartbeat(threadId int) {
	if h.expectedThreadHeartbeatThreadId != 0 {
		assert.Equal(h.t, h.expectedThreadHeartbeatThreadId, threadId)
	}

	h.ThreadHeartbeatFlag--
}

func (h *MockEventHandler) OnError(err *common.TaskThreadError) {
	if h.expectedErrorErr != nil {
		assert.Equal(h.t, h.expectedErrorErr, err)
	}

	h.ErrorFlag--
}

func (h *MockEventHandler) OnPreTask(task *common.Task) {
	if h.expectedPreTask != nil {
		assert.Equal(h.t, h.expectedPreTask, task)
	}

	h.PreTaskFlag--
}

func (h *MockEventHandler) OnPostTask(task *common.Task) {
	if h.expectedPostTask != nil {
		assert.Equal(h.t, h.expectedPostTask, task)
	}

	h.PostTagFlag--
}

func (h *MockEventHandler) OnTaskQueued(task *common.Task) {
	if h.expectedAcceptTaskTask != nil {
		assert.Equal(h.t, h.expectedAcceptTaskTask, task)
	}

	h.QueuedTaskFlag--
}

func (h *MockEventHandler) OnTaskAcceptTimeout(task *common.Task) {
	if h.expectedAcceptTimeoutTask != nil {
		assert.Equal(h.t, h.expectedAcceptTimeoutTask, task)
	}

	h.QueueTimeoutFlag--
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

func setupTest(t *testing.T, mock *Mock, eh *MockEventHandler) func() {
	// Test setup
	mock.t = t
	eh.t = t

	worker := NewWorker(mock.Configuration)
	worker.SetEventHandler(eh)

	mock.Worker = worker

	worker.StartServer()

	//wait for things to boot up
	time.Sleep(time.Millisecond * 5)

	// Test teardown - return a closure for use by 'defer'
	return func() {
		worker.StopServer()

		//wait for threads to clean up
		time.Sleep(time.Millisecond * 1)

		if eh.Flags != nil {
			util.AssertFlags(t, reflect.ValueOf(eh.Flags))
		}
	}
}

func TestLoadConfiguration(t *testing.T) {
	got, err := LoadConfiguration([]byte("{\"Concurrency\":4}"))

	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, 4, got.Concurrency)
}

func TestNewWorker(t *testing.T) {
	worker := NewWorker(nil)

	want := runtime.GOMAXPROCS(0)

	assert.Equal(t, want, cap(worker.taskQueue))
}

func TestStartServer(t *testing.T) {
	eh := &MockEventHandler{}

	mock := &Mock{Configuration: NewConfiguration()}
	mock.WaitTaskThreadsToClose = time.Second * 2

	defer setupTest(t, mock, eh)()
}

func TestHandleTask(t *testing.T) {
	flags := &Flags{PreTaskFlag: 1, PostTagFlag: 1, QueuedTaskFlag: 1}
	eh := &MockEventHandler{Flags: flags}

	mock := &Mock{Configuration: NewConfiguration()}

	defer setupTest(t, mock, eh)()

	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})
}

func TestHandleMultipleTask(t *testing.T) {
	flags := &Flags{PreTaskFlag: 10, PostTagFlag: 10, QueuedTaskFlag: 10}
	eh := &MockEventHandler{Flags: flags}

	mock := &Mock{Configuration: NewConfiguration()}

	defer setupTest(t, mock, eh)()

	for i := 0; i < 5; i++ {
		mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})
		mock.HandlePayload(&common.Task{Name: "long", Payload: []byte("{}")})
	}

	time.Sleep(time.Second)
}

func TestKillServer(t *testing.T) {
	flags := &Flags{PreTaskFlag: 1, QueuedTaskFlag: 1}
	eh := &MockEventHandler{Flags: flags}

	mock := &Mock{Configuration: NewConfiguration()}
	mock.WaitTaskThreadsToClose = time.Millisecond * 50

	defer setupTest(t, mock, eh)()

	mock.HandlePayload(&common.Task{Name: "long", Payload: []byte("{}")})
}

func TestHandlePayloadTimeout(t *testing.T) {
	config := NewConfiguration()

	//allow single task thread
	config.Concurrency = 2
	config.WaitToAcceptConsumerTask = time.Millisecond * 10

	flags := &Flags{PreTaskFlag: 4, PostTagFlag: 4, QueuedTaskFlag: 4, QueueTimeoutFlag: 1}
	eh := &MockEventHandler{Flags: flags}

	mock := &Mock{Configuration: config}

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
	config := NewConfiguration()

	//allow single task thread
	config.Concurrency = 1
	config.Heartbeat = time.Millisecond * 15

	flags := &Flags{PreTaskFlag: 2, PostTagFlag: 2, QueuedTaskFlag: 2, ThreadHeartbeatFlag: 1}
	eh := &MockEventHandler{Flags: flags}

	mock := &Mock{Configuration: config}

	defer setupTest(t, mock, eh)()

	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})

	//setting time between sending two tasks to allow for heartbeat
	time.Sleep(time.Millisecond * 40)

	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})

	time.Sleep(time.Millisecond * 20)
}

func TestTaskThreadError(t *testing.T) {
	config := NewConfiguration()

	flags := &Flags{PreTaskFlag: 3, PostTagFlag: 2, QueuedTaskFlag: 3, ErrorFlag: 1}
	eh := &MockEventHandler{Flags: flags}

	mock := &Mock{Configuration: config}

	defer setupTest(t, mock, eh)()

	mock.HandlePayload(&common.Task{Name: "long", Payload: []byte("{}")})
	mock.HandlePayload(&common.Task{Name: "error", Payload: []byte("{}")})
	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})

	time.Sleep(time.Millisecond * 200)
}
