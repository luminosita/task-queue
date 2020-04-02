package tasks

import (
	"encoding/json"
	"github.com/mnikita/task-queue/pkg/common"
	"testing"
)

func TestRegisteredTasks(t *testing.T) {
	payload, err := json.Marshal(&AddTask{A: 5, B: 6})

	if err != nil {
		t.Error(err)
	}

	handler, err := common.GetRegisteredTaskHandler(&common.Task{Name: "add", Payload: payload})

	if err != nil {
		t.Error(err)
	}

	err = handler.Handle()

	if err != nil {
		t.Error(err)
	}
}
