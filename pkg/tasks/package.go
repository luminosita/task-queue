package tasks

import "github.com/mnikita/task-queue/pkg/common"

const AddTaskName string = "add"

func init() {
	common.RegisterTask(AddTaskName, func() common.TaskHandler {
		return &AddTask{}
	})
}
