package workers

import (
	"github.com/mnikita/task-queue/pkg/common"
)

type EventHandler interface {
	OnError(err *common.TaskThreadError)

	OnPreTask(task *common.Task)

	OnPostTask(task *common.Task)

	OnThreadHeartbeat(threadId int)

	OnTaskQueued(task *common.Task)

	OnTaskAcceptTimeout(task *common.Task)
}
