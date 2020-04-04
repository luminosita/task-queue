//Package workers provides primitives for configuration and starting worker queues
package workers

import (
	"encoding/json"
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/mnikita/task-queue/pkg/log"
	"github.com/mnikita/task-queue/pkg/util"
	"runtime"
	"sync"
	"time"
)

//Worker stores configuration for server activation
type Worker struct {
	taskQueue chan *common.Task

	taskQueueQuit chan bool
	quit          chan bool

	config *Configuration

	eventHandler EventHandler

	taskQueueCounter int
	mux              sync.Mutex
}

//Configuration stores initialization data for worker server
type Configuration struct {
	//Number of pre-initialized task thread
	Concurrency int

	//Waiting time to accept new consumer task for processing
	WaitToAcceptConsumerTask time.Duration

	//Waiting time for worker to wait task threads closing
	WaitTaskThreadsToClose time.Duration

	//Waiting time to issue heartbeat
	Heartbeat time.Duration
}

func (w *Worker) handle(wg *sync.WaitGroup) {
	defer wg.Done()

	w.mux.Lock()
	w.taskQueueCounter++
	id := w.taskQueueCounter
	w.mux.Unlock()

	var runLoop = true

	log.Logger().TaskThreadStarted(id)
	defer log.Logger().TaskThreadEnded(id)

	for runLoop {
		select {
		case <-w.taskQueueQuit:
			runLoop = false
		case task := <-w.taskQueue:
			if task != nil {
				w.handleTask(id, task)
			} else {
				runLoop = false
			}
		case <-time.After(w.config.Heartbeat):
			w.OnThreadHeartbeat(id)
		}
	}
}

func (w *Worker) newTaskHandler(task *common.Task) (common.TaskHandler, error) {
	return common.GetRegisteredTaskHandler(task)
}

func (w *Worker) handleTask(threadId int, task *common.Task) {
	w.OnPreTask(task, threadId)

	taskHandler, err := w.newTaskHandler(task)

	if err != nil {
		w.OnError(common.NewTaskThreadError(task, threadId, err))
	}

	err = taskHandler.Handle()

	if err != nil {
		w.OnError(common.NewTaskThreadError(task, threadId, err))
	} else {
		w.OnPostTask(task, threadId)
	}
}

func getSystemConcurrency() (concurrency int) {
	return runtime.GOMAXPROCS(0)
}

func (w *Worker) startTaskThreads(waitGroup *sync.WaitGroup) {
	for i := 0; i < w.config.Concurrency; i++ {
		waitGroup.Add(1)
		go w.handle(waitGroup)
	}
}

func (w *Worker) stopTaskThreads(waitGroup *sync.WaitGroup) {
	log.Logger().TaskThreadsStopping(w.config.Concurrency)

	close(w.taskQueue)

	for i := 0; i < w.config.Concurrency; i++ {
		w.taskQueueQuit <- true
	}

	err := util.WaitTimeout(waitGroup, w.config.WaitTaskThreadsToClose)

	if err != nil {
		log.Logger().Error(err)
	}
}

//LoadConfiguration loads external confirmation
func LoadConfiguration(configData []byte) (config *Configuration, err error) {
	config = NewConfiguration()

	err = json.Unmarshal(configData, config)

	if err != nil {
		return nil, err
	}

	return config, nil
}

func NewConfiguration() *Configuration {
	//make default configuration
	return &Configuration{
		Concurrency:              getSystemConcurrency(),
		Heartbeat:                time.Second * 5,
		WaitToAcceptConsumerTask: time.Second * 30,
		WaitTaskThreadsToClose:   time.Second * 30,
	}
}

//NewWorker creates and configures Worker instance
func NewWorker(config *Configuration) *Worker {
	if config == nil {
		config = NewConfiguration()
	}

	var taskQueue = make(chan *common.Task, config.Concurrency)

	var quit = make(chan bool)
	var taskQueueQuit = make(chan bool, config.Concurrency)

	worker := &Worker{config: config, taskQueue: taskQueue, quit: quit, taskQueueQuit: taskQueueQuit}

	return worker
}

//SetWorkerHandlers sets error, pre and post task handlers
func (w *Worker) SetEventHandler(eventHandler EventHandler) {
	w.eventHandler = eventHandler
}

//StartServer starts workers server
func (w *Worker) StartServer() {
	log.Logger().WorkerStarted()
	var waitGroup sync.WaitGroup

	w.startTaskThreads(&waitGroup)

	go func(wg *sync.WaitGroup) {
		<-w.quit
		w.stopTaskThreads(wg)

		w.quit <- true

		close(w.quit)

		return
	}(&waitGroup)
}

//StopServer stops workers server
func (w *Worker) StopServer() {
	log.Logger().WorkerStopping()

	//send stop signal to worker thread
	w.quit <- true

	//wait for worker thread stop confirmation
	<-w.quit

	log.Logger().WorkerEnded()
}

//Handles task payload from consumer
func (w *Worker) HandlePayload(task *common.Task) {
	select {
	case w.taskQueue <- task:
		w.OnTaskQueued(task)
	case <-time.After(w.config.WaitToAcceptConsumerTask):
		w.OnTaskAcceptTimeout(task)
	}
}

func (w *Worker) OnError(err *common.TaskThreadError) {
	log.Logger().Error(err)

	if w.eventHandler != nil {
		w.eventHandler.OnError(err)
	}
}

func (w *Worker) OnPreTask(task *common.Task, threadId int) {
	log.Logger().TaskPreHandler(task.Name, threadId)

	if w.eventHandler != nil {
		w.eventHandler.OnPreTask(task)
	}
}

func (w *Worker) OnPostTask(task *common.Task, threadId int) {
	log.Logger().TaskPostHandler(task.Name, threadId)

	if w.eventHandler != nil {
		w.eventHandler.OnPostTask(task)
	}
}

func (w *Worker) OnThreadHeartbeat(threadId int) {
	log.Logger().ThreadHeartbeat(threadId, w.config.Heartbeat)

	if w.eventHandler != nil {
		w.eventHandler.OnThreadHeartbeat(threadId)
	}
}

func (w *Worker) OnTaskQueued(task *common.Task) {
	log.Logger().TaskQueued(task.Name)

	if w.eventHandler != nil {
		w.eventHandler.OnTaskQueued(task)
	}
}

func (w *Worker) OnTaskAcceptTimeout(task *common.Task) {
	log.Logger().TaskQueueTimeout(task.Name, w.config.WaitToAcceptConsumerTask)

	if w.eventHandler != nil {
		w.eventHandler.OnTaskAcceptTimeout(task)
	}
}
