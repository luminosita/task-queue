package workers

import (
	"github.com/mnikita/task-queue/common"
	"log"
	"runtime"
)

type Worker struct {
	connection *common.Connection
	taskQueue  chan *common.Task

	concurrency int

	errorHandler    func(task *common.Task, err error)
	preTaskHandler  func(task *common.Task)
	postTaskHandler func(task *common.Task)
}

type Configuration struct {
	concurrency int
}

func loadConfiguration() (config *Configuration, err error) {
	config = &Configuration{}

	//TODO return proper Error
	err = nil

	return
}

func (w *Worker) StopConsuming(quit chan bool) (err error) {
	quit <- true

	return nil
}

func getSystemConcurrency() (concurrency int) {
	return runtime.GOMAXPROCS(0)
}

func NewWorker(conn *common.Connection) (*Worker, error) {
	config, err := loadConfiguration()

	if err != nil {
		return nil, err
	}

	concurrency := config.concurrency

	if concurrency == 0 {
		concurrency = getSystemConcurrency()
	}

	var queue = make(chan *common.Task, concurrency)

	worker := &Worker{connection: conn, taskQueue: queue, concurrency: concurrency}

	worker.errorHandler = func(task *common.Task, err error) {
		log.Println(err)
	}

	worker.preTaskHandler = func(task *common.Task) {
		log.Printf("%s PRE HANDLER !!!", task.Name)
	}

	worker.postTaskHandler = func(task *common.Task) {
		log.Printf("%s POST HANDLER !!!", task.Name)
	}

	return worker, err
}

func (w *Worker) StartConsuming(quit chan bool) {
	for i := 0; i < w.concurrency; i++ {
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

	err = taskHandler.Handle()

	if err != nil {
		w.errorHandler(task, err)
	}

	w.postTaskHandler(task)
}
