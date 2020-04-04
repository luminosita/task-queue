//Package log provides primitives for structured log generation.
//It declares all available error and log messages for the module
package log

import (
	"flag"
	"fmt"
	"github.com/sirupsen/logrus"
	"os"
	"strconv"
	"sync"
	"time"
)

//Event stores messages to log later, from our standard interface
type Event struct {
	//	id      int
	message string
}

//Error stores messages to log later, from our standard interface
type Error struct {
	message string
}

//StandardLogger enforces specific log message formats
type StandardLogger struct {
	*logrus.Logger
}

type loggerSingleton *StandardLogger

var (
	once   sync.Once
	logger loggerSingleton
)

type ErrorId int
type MessageId int

//errors
var (
	registeredTaskHandler = Event{"RegisteredTaskHandler(%s): unknown task name"}
	task                  = Event{"TaskError(%s): %s"}
	taskThread            = Event{"TaskError(%s) in thread(%d): %s"}
	workerWaitTimeout     = Event{"Timed out waiting for task threads to close after %d seconds"}
)

//messages
var (
	taskRegistered      = Event{"Task registered: %s"}
	taskPreHandler      = Event{"Task PreHandler(%s), thread(%d)"}
	taskPostHandler     = Event{"Task PostHandler(%s), thread(%d)"}
	taskThreadStarted   = Event{"Task thread(%d) started"}
	taskThreadEnded     = Event{"Task thread(%d) ended"}
	taskThreadWaitQuit  = Event{"Waiting on task thread to end after %d seconds"}
	workerStarted       = Event{"Worker started"}
	workerStopping      = Event{"Worker stopping"}
	workerEnded         = Event{"Worker ended"}
	taskQueued          = Event{"Task(%s) queued"}
	taskQueueTimeout    = Event{"Task(%s) queue timeout after %d seconds. Retrying ..."}
	consumerStarted     = Event{"Consumer started"}
	consumerStopping    = Event{"Consumer stopping"}
	consumerEnded       = Event{"Consumer ended"}
	taskThreadsStopping = Event{"Task threads (%d) stopping ..."}
	threadHeartbeat     = Event{"Task thread (%d) heartbeat after %d seconds"}
)

func (id ErrorId) String() string {
	return [...]string{
		"RegisteredTaskHandler(%s): unknown task name",
		"TaskError(%s): %s",
		"Timed out waiting for task threads to close after %d seconds",
	}[id]
}

//Logger initializes the standard logger
func Logger() *StandardLogger {
	once.Do(func() { // <-- atomic, does not allow repeating
		var baseLogger = logrus.New()

		logger = &StandardLogger{baseLogger}

		// Log as JSON instead of the default ASCII formatter.
		logger.Formatter = &logrus.JSONFormatter{}

		// Output to stdout instead of the default stderr, could also be a file.
		logger.Out = os.Stdout

		// Only log the warning severity or above.
		logger.Level = logrus.InfoLevel

		if logLevelArg, err := strconv.Atoi(flag.Arg(0)); err == nil {
			logger.Level = logrus.AllLevels[logLevelArg]
		}
	})

	return logger
}

//Error provides implementation of Error interface
func (e *Error) Error() string {
	return e.message
}

//Error message
func RegisteredTaskHandlerError(taskName string) error {
	return &Error{fmt.Sprintf(registeredTaskHandler.message, taskName)}
}

//Error message
func TaskError(taskName string, err error) error {
	return &Error{fmt.Sprintf(task.message, taskName, err)}
}

//Error message
func TaskThreadError(taskName string, threadId int, err error) error {
	return &Error{fmt.Sprintf(taskThread.message, taskName, threadId, err)}
}

//Error message
func WorkerWaitTimeoutError(secs time.Duration) error {
	return &Error{fmt.Sprintf(workerWaitTimeout.message, secs/time.Second)}
}

//Log message
func (l *StandardLogger) TaskRegistered(taskName string) {
	l.Infof(taskRegistered.message, taskName)
}

//Log message
func (l *StandardLogger) TaskPreHandler(taskName string, threadId int) {
	l.Infof(taskPreHandler.message, taskName, threadId)
}

//Log message
func (l *StandardLogger) TaskPostHandler(taskName string, threadId int) {
	l.Infof(taskPostHandler.message, taskName, threadId)
}

//Log message
func (l *StandardLogger) TaskThreadWaitQuit(secs time.Duration) {
	l.Infof(taskThreadWaitQuit.message, secs/time.Second)
}

//Log message
func (l *StandardLogger) TaskThreadStarted(id int) {
	l.Infof(taskThreadStarted.message, id)
}

//Log message
func (l *StandardLogger) TaskThreadsStopping(count int) {
	l.Infof(taskThreadsStopping.message, count)
}

//Log message
func (l *StandardLogger) TaskThreadEnded(id int) {
	l.Infof(taskThreadEnded.message, id)
}

//Log message
func (l *StandardLogger) TaskQueued(name string) {
	l.Infof(taskQueued.message, name)
}

//Log message
func (l *StandardLogger) TaskQueueTimeout(name string, secs time.Duration) {
	l.Infof(taskQueueTimeout.message, name, secs/time.Second)
}

//Log message
func (l *StandardLogger) WorkerStarted() {
	l.Infof(workerStarted.message)
}

//Log message
func (l *StandardLogger) WorkerStopping() {
	l.Infof(workerStopping.message)
}

//Log message
func (l *StandardLogger) WorkerEnded() {
	l.Infof(workerEnded.message)
}

//Log message
func (l *StandardLogger) ConsumerStarted() {
	l.Infof(consumerStarted.message)
}

//Log message
func (l *StandardLogger) ConsumerStopping() {
	l.Infof(consumerStopping.message)
}

//Log message
func (l *StandardLogger) ConsumerEnded() {
	l.Infof(consumerEnded.message)
}

//Log message
func (l *StandardLogger) ThreadHeartbeat(threadId int, secs time.Duration) {
	l.Infof(threadHeartbeat.message, threadId, secs/time.Second)
}
