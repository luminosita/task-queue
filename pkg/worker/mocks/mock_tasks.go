package mocks

import (
	"errors"
	"github.com/mnikita/task-queue/pkg/common"
	"time"
)

const (
	Short = iota
	ShortResult
	Long
	Error
	Heartbeat
)

var Tasks = []string{"Short", "ShortResult", "Long", "Error", "Heartbeat"}

var ErrorTaskErr = errors.New("ErrorTask test error")

var ShortTaskResult = "ShortTaskResult"

func HandleShortTest(_ *common.Task, _ common.TaskProcessEventHandler) error {
	time.Sleep(time.Millisecond * 20)
	return nil
}

func HandleShortTestWithResult(task *common.Task, handler common.TaskProcessEventHandler) error {
	time.Sleep(time.Millisecond * 20)

	handler.OnTaskResult(task, ShortTaskResult)

	return nil
}

func HandleLongTask(_ *common.Task, _ common.TaskProcessEventHandler) error {
	time.Sleep(time.Millisecond * 500)

	return nil
}

func HandleHeartbeatTask(task *common.Task, eventHandler common.TaskProcessEventHandler) error {
	time.Sleep(time.Millisecond * 100)

	eventHandler.OnTaskHeartbeat(task)

	time.Sleep(time.Millisecond * 100)

	return nil
}

func HandleErrorTask(_ *common.Task, _ common.TaskProcessEventHandler) error {
	time.Sleep(time.Millisecond * 10)

	return ErrorTaskErr
}

func RegisterTasks() {
	common.RegisterTask(Tasks[Short], func() common.TaskHandler {
		return common.NewBaseTaskHandler(HandleShortTest)
	})

	common.RegisterTask(Tasks[ShortResult], func() common.TaskHandler {
		return common.NewBaseTaskHandler(HandleShortTestWithResult)
	})

	common.RegisterTask(Tasks[Long], func() common.TaskHandler {
		return common.NewBaseTaskHandler(HandleLongTask)
	})

	common.RegisterTask(Tasks[Error], func() common.TaskHandler {
		return common.NewBaseTaskHandler(HandleErrorTask)
	})

	common.RegisterTask(Tasks[Heartbeat], func() common.TaskHandler {
		return common.NewBaseTaskHandler(HandleHeartbeatTask)
	})
}
