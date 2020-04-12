package mocks

import (
	"errors"
	"github.com/mnikita/task-queue/pkg/common"
	"github.com/mnikita/task-queue/pkg/log"
	"time"
)

const (
	Short = iota
	ShortResult
	Long
	Error
	Heartbeat
	Payload
)

var Tasks = []string{"Short", "ShortResult", "Long", "Error", "Heartbeat", "Payload"}

var ErrorTaskErr = errors.New("ErrorTask test error")

var ShortTaskResult = "ShortTaskResult"

type TestPayload struct {
	Mika int    `json:"mika"`
	Pera int    `json:"pera"`
	Laza string `json:"laza"`
}

func HandleShortTest(_ interface{}, _ *common.Task, _ common.TaskProcessEventHandler) error {
	time.Sleep(time.Millisecond * 20)
	return nil
}

func HandleShortTestWithResult(_ interface{}, task *common.Task,
	handler common.TaskProcessEventHandler) error {
	time.Sleep(time.Millisecond * 20)

	handler.OnTaskResult(task, ShortTaskResult)

	return nil
}

func HandleLongTask(_ interface{}, _ *common.Task, _ common.TaskProcessEventHandler) error {
	time.Sleep(time.Millisecond * 500)

	return nil
}

func HandleHeartbeatTask(_ interface{}, task *common.Task, eventHandler common.TaskProcessEventHandler) error {
	time.Sleep(time.Millisecond * 100)

	eventHandler.OnTaskHeartbeat(task)

	time.Sleep(time.Millisecond * 100)

	return nil
}

func HandleErrorTask(_ interface{}, _ *common.Task, _ common.TaskProcessEventHandler) error {
	time.Sleep(time.Millisecond * 10)

	return ErrorTaskErr
}

func HandlePayloadTask(payload interface{}, _ *common.Task, _ common.TaskProcessEventHandler) error {
	time.Sleep(time.Millisecond * 20)

	log.Logger().Infof("Task with Payload %+v", payload)

	return nil
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

	common.RegisterTask(Tasks[Payload], func() common.TaskHandler {
		payload := new(TestPayload)

		return common.NewBaseTaskHandlerWithPayload(HandlePayloadTask, payload)
	})
}
