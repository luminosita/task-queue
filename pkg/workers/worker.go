//Package workers provides primitives for configuration and starting worker queues
package workers

import (
	"encoding/json"
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/mnikita/task-queue/pkg/log"
	"runtime"
)

//ErrorHandler defines worker error handler
type ErrorHandler func(task *common.Task, err error)

//PreTaskHandler defines worker pre task handler
type PreTaskHandler func(task *common.Task)

//PostTaskHandler defines worker post task handler
type PostTaskHandler func(task *common.Task)

//Worker stores configuration for server activation
type Worker struct {
	connection *common.Connection
	taskQueue  chan *common.Task

	config *Configuration

	errorHandler    ErrorHandler
	preTaskHandler  PreTaskHandler
	postTaskHandler PostTaskHandler
}

//Configuration stores initialization data for worker server
type Configuration struct {
	Concurrency int
}

//LoadConfiguration loads external confirmation
func LoadConfiguration(configData []byte) (config *Configuration, err error) {
	config = &Configuration{}

	err = json.Unmarshal(configData, config)

	if err != nil {
		return nil, err
	}

	return config, nil
}

func getSystemConcurrency() (concurrency int) {
	return runtime.GOMAXPROCS(0)
}

//StopConsuming stops workers server
func (w *Worker) StopConsuming(quit chan bool) (err error) {
	quit <- true

	return nil
}

//NewWorker creates and configures Worker instance
func NewWorker(conn *common.Connection, config *Configuration) (worker *Worker) {
	concurrency := config.Concurrency

	if concurrency == 0 {
		concurrency = getSystemConcurrency()
	}

	var queue = make(chan *common.Task, concurrency)

	worker = &Worker{connection: conn, taskQueue: queue, config: config}

	worker.errorHandler = func(task *common.Task, err error) {
		log.Logger().Error(err)
	}

	worker.preTaskHandler = func(task *common.Task) {
		log.Logger().DefaultPreHandler(task.Name)
	}

	worker.postTaskHandler = func(task *common.Task) {
		log.Logger().DefaultPostHandler(task.Name)
	}

	return worker
}

//SetWorkerHandlers sets error, pre and post task handlers
func (w *Worker) SetWorkerHandlers(errorHandler ErrorHandler, preTaskHandler PreTaskHandler, postTaskHandler PostTaskHandler) {
	w.errorHandler = errorHandler
	w.preTaskHandler = preTaskHandler
	w.postTaskHandler = postTaskHandler
}

//StartConsuming starts workers server
func (w *Worker) StartConsuming(quit chan bool) {
	for i := 0; i < w.config.Concurrency; i++ {
		go w.handle()
	}

	// Wait to be told to exit.
	<-quit
}

func (w *Worker) handle() {
	for task := range w.taskQueue {
		w.handleTask(task)
	}
}

func (w *Worker) newTaskHandler(task *common.Task) (common.TaskHandler, error) {
	return common.GetRegisteredTaskHandler(task)
}

func (w *Worker) handleTask(task *common.Task) {
	taskHandler, err := w.newTaskHandler(task)

	if err != nil {
		w.errorHandler(task, err)
	}

	w.preTaskHandler(task)

	err = taskHandler.Handle(nil)

	if err != nil {
		w.errorHandler(task, err)
	}

	w.postTaskHandler(task)
}

//TODO: CHECK IMPLEMENTATION
//type Server struct{ quit chan bool }
//
//func NewServer() *Server {
//	s := &Server{make(chan bool)}
//	go s.run()
//	return s
//}
//
//func (s *Server) run() {
//	for {
//		select {
//		case <-s.quit:
//			fmt.Println("finishing task")
//			time.Sleep(time.Second)
//			fmt.Println("task done")
//			s.quit <- true
//			return
//		case <-time.After(time.Second):
//			fmt.Println("running task")
//		}
//	}
//}
//func (s *Server) Stop() {
//	fmt.Println("server stopping")
//	s.quit <- true
//	<-s.quit
//	fmt.Println("server stopped")
//}
//
//func main() {
//	s := NewServer()
//	time.Sleep(2 * time.Second)
//	s.Stop()
//}
