package common

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
)

type RegisteredTasksError struct {
	TaskName string
}

type TaskError struct {
	Task *Task
	Err  error
}

type registeredTasksSingleton map[string]TaskConstructor

var (
	once            sync.Once
	registeredTasks registeredTasksSingleton
)

func (e *RegisteredTasksError) Error() string {
	return fmt.Sprintf("GetRegisteredTaskHandler(%s): unknown task name", e.TaskName)
}

func (e *TaskError) Error() string {
	return fmt.Sprintf("TaskError(%s): %s", e.Task.Name, e.Err)
}

func newRegisteredTasks() registeredTasksSingleton {

	once.Do(func() { // <-- atomic, does not allow repeating

		registeredTasks = make(registeredTasksSingleton) // <-- thread safe

	})

	return registeredTasks
}

func RegisterTask(taskName string, constructor TaskConstructor) {
	log.Printf("Task registered: %s", taskName)

	newRegisteredTasks()[taskName] = constructor
}

func unmarshalTask(task *Task, taskHandler TaskHandler) (err error) {
	err = json.Unmarshal(task.Payload, taskHandler)

	if err != nil {
		return &TaskError{Task: task, Err: err}
	}

	return nil
}

func GetRegisteredTaskHandler(task *Task) (TaskHandler, error) {
	constructor, ok := newRegisteredTasks()[task.Name]

	if !ok {
		return nil, &RegisteredTasksError{TaskName: task.Name}
	}

	taskHandler := constructor()

	err := unmarshalTask(task, taskHandler)

	return taskHandler, err
}
