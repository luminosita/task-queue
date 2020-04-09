package common

import (
	"encoding/json"
	"github.com/mnikita/task-queue/pkg/log"
	"github.com/thoas/go-funk"
	"sync"
)

type registeredTasksSingleton map[string]TaskConstructor

var (
	once            sync.Once
	registeredTasks registeredTasksSingleton
)

func newRegisteredTasks() registeredTasksSingleton {

	once.Do(func() { // <-- atomic, does not allow repeating

		registeredTasks = make(registeredTasksSingleton) // <-- thread safe

	})

	return registeredTasks
}

//RegisterTask registers tasks by name and stores TaskConstructor for TaskHandler instances creation
func RegisterTask(taskName string, constructor TaskConstructor) {
	log.Logger().TaskRegistered(taskName)

	newRegisteredTasks()[taskName] = constructor
}

//GetRegisteredTaskHandler retrieves TaskHandlers by name.
//Task request payload is unmarshalled to initialize TaskHandler
func GetRegisteredTaskHandler(task *Task) (TaskHandler, error) {
	constructor, ok := newRegisteredTasks()[task.Name]

	if !ok {
		return nil, log.RegisteredTaskHandlerError(task.Name)
	}

	taskHandler := constructor()

	err := json.Unmarshal(task.Payload, taskHandler)

	if err != nil {
		return nil, log.InvalidTaskPayloadError(task.Id, task.Name, err)
	}

	return taskHandler, err
}

//GetRegisteredTasks returns slice with all task names registered
func GetRegisteredTasks() []string {
	return funk.Keys(newRegisteredTasks()).([]string)
}
