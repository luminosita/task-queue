package tasks

import "github.com/mnikita/task-queue/common"

const AddTaskName string = "add"

func init() {
	common.RegisterTask(AddTaskName, func() common.TaskHandler {
		return &AddTask{}
	})
}
