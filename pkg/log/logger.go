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
	missingConsumerHandler    = Event{"ConsumerHandler not specified"}
	missingTaskPayloadHandler = Event{"TaskPayloadHandler not specified"}
	registeredTaskHandler     = Event{"RegisteredTaskHandler(%s): unknown task name"}
	taskThread                = Event{"Task(%s) failed in thread(%d): %s"}
	workerWaitTimeout         = Event{"Timed out waiting for taskFailed threads to close after %d seconds"}

	emptyReserveTaskPayload   = Event{"Task(%d) payload empty"}
	invalidReserveTaskPayload = Event{"Invalid Reserved Task(%d) JSON format: %s"}
	invalidTaskPayload        = Event{"Invalid Task(id: %d, name: %s) payload JSON format: %s"}
)

//messages
var (
	taskRegistered      = Event{"Task registered: %s"}
	taskPre             = Event{"Task PreHandler(%s), thread(%d)"}
	taskPost            = Event{"Task PostHandler(%s), thread(%d)"}
	taskThreadStarted   = Event{"Task thread(%d) started"}
	taskThreadEnded     = Event{"Task thread(%d) ended"}
	taskThreadWaitQuit  = Event{"Waiting on task thread to end after %d seconds"}
	taskQueued          = Event{"Task(%s) queued"}
	taskQueueTimeout    = Event{"Task(%s) queue timeout after %d seconds. Retrying ..."}
	taskThreadsStopping = Event{"Task threads (%d) stopping ..."}
	threadHeartbeat     = Event{"Task thread (%d) heartbeat after %d seconds"}

	workerStarted  = Event{"Worker started"}
	workerStopping = Event{"Worker stopping"}
	workerEnded    = Event{"Worker ended"}

	consumerStarted        = Event{"Consumer started"}
	consumerStopping       = Event{"Consumer stopping"}
	consumerEnded          = Event{"Consumer ended"}
	consumerReserveTimeout = Event{"Reserve timeout after %d seconds"}
	consumerHeartbeat      = Event{"Consumer heartbeat after %d seconds"}

	taskProcessEvent        = Event{"Task event (%s) received: (%s)"}
	taskProcessEventTimeout = Event{"Task event (%s) timeout after (%s) seconds: (%s)"}

	consumerReserve = Event{"Reserve (timeout: %d seconds)"}
	consumerRelease = Event{"Release (Id: %d, Priority: (%d), Delay: (%d seconds))"}
	consumerBury    = Event{"Bury (Id: (%d), Priority: (%d))"}
	consumerTouch   = Event{"Touch (Id: (%d))"}
	consumerDelete  = Event{"Delete (Id: (%d))"}
	consumerClose   = Event{"Close consumer connection"}

	configWatchError    = Event{"Configuration watcher error: %s"}
	configWatchModified = Event{"Configuration file modified: %s"}
	configWatchStart    = Event{"Configuration watch started"}
	configWatchStop     = Event{"Configuration watch stopped"}
	configWatchFile     = Event{"Configuration watch added file: %s"}

	beanConfigLoaded          = Event{"Configuration loaded successfully: %s"}
	beanUrl                   = Event{"URL configured: %s"}
	beanConnectionEstablished = Event{"Connection successfully established. Listen on tubes %s"}

	reservedTaskBody = Event{"Body of reserved task: (%s)"}
)

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
func MissingConsumerHandlerError() error {
	return &Error{missingConsumerHandler.message}
}

//Error message
func MissingTaskPayloadHandlerError() error {
	return &Error{missingTaskPayloadHandler.message}
}

//Error message
func RegisteredTaskHandlerError(taskName string) error {
	return &Error{fmt.Sprintf(registeredTaskHandler.message, taskName)}
}

//Error message
func EmptyReserveTaskPayloadError(id uint64) error {
	return &Error{fmt.Sprintf(emptyReserveTaskPayload.message, id)}
}

//Error message
func InvalidReserveTaskPayloadError(id uint64, err error) error {
	return &Error{fmt.Sprintf(invalidReserveTaskPayload.message, id, err)}
}

//Error message
func InvalidTaskPayloadError(id uint64, taskName string, err error) error {
	return &Error{fmt.Sprintf(invalidTaskPayload.message, id, taskName, err)}
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
func (l *StandardLogger) TaskPre(taskName string, threadId int) {
	l.Infof(taskPre.message, taskName, threadId)
}

//Log message
func (l *StandardLogger) TaskPost(taskName string, threadId int) {
	l.Infof(taskPost.message, taskName, threadId)
}

//Log message
func (l *StandardLogger) TaskProcessEvent(eventType string, taskName string) {
	l.Infof(taskProcessEvent.message, eventType, taskName)
}

//Log message
func (l *StandardLogger) TaskProcessEventTimeout(eventType string, taskName string, secs time.Duration) {
	l.Infof(taskProcessEventTimeout.message, eventType, secs/time.Second, taskName)
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
func (l *StandardLogger) ConsumerReserveTimeout(secs time.Duration) {
	l.Infof(consumerReserveTimeout.message, secs/time.Second)
}

//Log message
func (l *StandardLogger) ConsumerHeartbeat(secs time.Duration) {
	l.Infof(consumerHeartbeat.message, secs/time.Second)
}

//Log message
func (l *StandardLogger) ThreadHeartbeat(threadId int, secs time.Duration) {
	l.Infof(threadHeartbeat.message, threadId, secs/time.Second)
}

//Log message
func (l *StandardLogger) ConsumerReserve(timeout time.Duration) {
	l.Infof(consumerReserve.message, timeout/time.Second)
}

//Log message
func (l *StandardLogger) ConsumerRelease(id uint64, pri uint32, delay time.Duration) {
	l.Infof(consumerRelease.message, id, pri, delay/time.Second)
}

//Log message
func (l *StandardLogger) ConsumerBury(id uint64, pri uint32) {
	l.Infof(consumerBury.message, id, pri)
}

//Log message
func (l *StandardLogger) ConsumerTouch(id uint64) {
	l.Infof(consumerTouch.message, id)
}

//Log message
func (l *StandardLogger) ConsumerDelete(id uint64) {
	l.Infof(consumerDelete.message, id)
}

//Log message
func (l *StandardLogger) ConsumerClose() {
	l.Infof(consumerClose.message)
}

//Log message
func (l *StandardLogger) ConfigWatchError(err error) {
	l.Infof(configWatchError.message, err.Error())
}

//Log message
func (l *StandardLogger) ConfigWatchModified(path string) {
	l.Infof(configWatchModified.message, path)
}

//Log message
func (l *StandardLogger) ConfigWatchStart() {
	l.Infof(configWatchStart.message)
}

//Log message
func (l *StandardLogger) ConfigWatchStop() {
	l.Infof(configWatchStop.message)
}

//Log message
func (l *StandardLogger) ConfigWatchFile(path string) {
	l.Infof(configWatchFile.message, path)
}

//Log message
func (l *StandardLogger) BeanConfigLoaded(path string) {
	l.Infof(beanConfigLoaded.message, path)
}

//Log message
func (l *StandardLogger) BeanUrl(url string) {
	l.Infof(beanUrl.message, url)
}

//Log message
func (l *StandardLogger) BeanConnectionEstablished(tubes []string) {
	l.Infof(beanConnectionEstablished.message, tubes)
}

//Log message
func (l *StandardLogger) ReservedTaskBody(body string) {
	l.Infof(reservedTaskBody.message, body)
}
