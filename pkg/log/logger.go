//Package log provides primitives for structured log generation.
//It declares all available error and log messages for the module
package log

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"sync"
)

//Event stores messages to log later, from our standard interface
type Event struct {
	id      int
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

//errors
var (
	registeredTaskHandlerError = Event{1001, "RegisteredTaskHandler(%s): unknown task name"}
	taskError                  = Event{1002, "TaskError(%s): %s"}
)

//messages
var (
	taskRegistered     = Event{2001, "Task registered: %s"}
	defaultPreHandler  = Event{2002, "%s PRE HANDLER !!!"}
	defaultPostHandler = Event{2003, "%s POST HANDLER !!!"}
)

//Logger initializes the standard logger
func Logger() *StandardLogger {
	once.Do(func() { // <-- atomic, does not allow repeating
		var baseLogger = logrus.New()

		logger = &StandardLogger{baseLogger}

		logger.Formatter = &logrus.JSONFormatter{}
	})

	return logger
}

//Error provides implementation of Error interface
func (e *Error) Error() string {
	return e.message
}

//Error message
func RegisteredTaskHandlerError(taskName string) error {
	return &Error{fmt.Sprintf(registeredTaskHandlerError.message, taskName)}
}

//Error message
func TaskError(taskName string, err error) error {
	return &Error{fmt.Sprintf(taskError.message, taskName, err)}
}

//Log message
func (l *StandardLogger) TaskRegistered(taskName string) {
	l.Infof(taskRegistered.message, taskName)
}

//Log message
func (l *StandardLogger) DefaultPreHandler(taskName string) {
	l.Infof(defaultPreHandler.message, taskName)
}

//Log message
func (l *StandardLogger) DefaultPostHandler(taskName string) {
	l.Infof(defaultPostHandler.message, taskName)
}
