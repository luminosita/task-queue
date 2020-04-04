package workers

import (
	"errors"
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/mnikita/task-queue/pkg/log"
	"github.com/stretchr/testify/assert"
	"os"
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

	threadHeartbeatFlag   int
	errorFlag             int
	preTaskFlag           int
	postTagFlag           int
	acceptTaskFlag        int
	acceptTaskTimeoutFlag int
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

	h.threadHeartbeatFlag--
}

func (h *MockEventHandler) OnError(err *common.TaskThreadError) {
	if h.expectedErrorErr != nil {
		assert.Equal(h.t, h.expectedErrorErr, err)
	}

	h.errorFlag--
}

func (h *MockEventHandler) OnPreTask(task *common.Task) {
	if h.expectedPreTask != nil {
		assert.Equal(h.t, h.expectedPreTask, task)
	}

	h.preTaskFlag--
}

func (h *MockEventHandler) OnPostTask(task *common.Task) {
	if h.expectedPostTask != nil {
		assert.Equal(h.t, h.expectedPostTask, task)
	}

	h.postTagFlag--
}

func (h *MockEventHandler) OnTaskQueued(task *common.Task) {
	if h.expectedAcceptTaskTask != nil {
		assert.Equal(h.t, h.expectedAcceptTaskTask, task)
	}

	h.acceptTaskFlag--
}

func (h *MockEventHandler) OnTaskAcceptTimeout(task *common.Task) {
	if h.expectedAcceptTimeoutTask != nil {
		assert.Equal(h.t, h.expectedAcceptTimeoutTask, task)
	}

	h.acceptTaskTimeoutFlag--
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

		assert.Equal(t, 0, eh.errorFlag)
		assert.Equal(t, 0, eh.preTaskFlag)
		assert.Equal(t, 0, eh.postTagFlag)
		assert.Equal(t, 0, eh.threadHeartbeatFlag)
		assert.Equal(t, 0, eh.acceptTaskFlag)
		assert.Equal(t, 0, eh.acceptTaskTimeoutFlag)
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
	eh := &MockEventHandler{preTaskFlag: 1, postTagFlag: 1, acceptTaskFlag: 1}

	mock := &Mock{Configuration: NewConfiguration()}

	defer setupTest(t, mock, eh)()

	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})
}

func TestHandleMultipleTask(t *testing.T) {
	eh := &MockEventHandler{preTaskFlag: 10, postTagFlag: 10, acceptTaskFlag: 10}

	mock := &Mock{Configuration: NewConfiguration()}

	defer setupTest(t, mock, eh)()

	for i := 0; i < 5; i++ {
		mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})
		mock.HandlePayload(&common.Task{Name: "long", Payload: []byte("{}")})
	}

	time.Sleep(time.Second)
}

func TestKillServer(t *testing.T) {
	eh := &MockEventHandler{preTaskFlag: 1, acceptTaskFlag: 1}

	mock := &Mock{Configuration: NewConfiguration()}
	mock.WaitTaskThreadsToClose = time.Millisecond * 50

	defer setupTest(t, mock, eh)()

	mock.HandlePayload(&common.Task{Name: "long", Payload: []byte("{}")})
}

func TestHandlePayloadTimeout(t *testing.T) {
	config := NewConfiguration()

	//allow single task thread
	config.Concurrency = 1
	config.WaitToAcceptConsumerTask = time.Millisecond * 10

	eh := &MockEventHandler{preTaskFlag: 2, postTagFlag: 2, acceptTaskFlag: 2, acceptTaskTimeoutFlag: 1}

	mock := &Mock{Configuration: config}

	defer setupTest(t, mock, eh)()

	//first task should get processed
	mock.HandlePayload(&common.Task{Name: "long", Payload: []byte("{}")})

	//second will be queued
	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})
	//third should hang and issue timeout
	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})

	time.Sleep(time.Second)
}

func TestHeartbeat(t *testing.T) {
	config := NewConfiguration()

	//allow single task thread
	config.Concurrency = 1
	config.Heartbeat = time.Millisecond * 10

	eh := &MockEventHandler{preTaskFlag: 2, postTagFlag: 2, acceptTaskFlag: 2, threadHeartbeatFlag: 1}

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

	eh := &MockEventHandler{preTaskFlag: 3, postTagFlag: 2, acceptTaskFlag: 3, errorFlag: 1}

	mock := &Mock{Configuration: config}

	defer setupTest(t, mock, eh)()

	mock.HandlePayload(&common.Task{Name: "long", Payload: []byte("{}")})
	mock.HandlePayload(&common.Task{Name: "error", Payload: []byte("{}")})
	mock.HandlePayload(&common.Task{Name: "test", Payload: []byte("{}")})

	time.Sleep(time.Millisecond * 200)
}
