package workers

import (
	"encoding/json"
	"fmt"
	"github.com/mnikita/task-queue/pkg/common"
	"runtime"
	"testing"
)

func TestLoadConfiguration(t *testing.T) {
	bytes, err := json.Marshal(&Configuration{Concurrency: 4})

	if err != nil {
		t.Error(err)
	}

	fmt.Println(string(bytes))

	got, err := LoadConfiguration([]byte("{\"Concurrency\":4}"))

	if err != nil {
		t.Error(err)
	}

	if got.Concurrency != 4 {
		t.Errorf("LoadConfiguration(%q) == %q, want %q", "concurrency", got.Concurrency, 4)
	}
}

func TestNewWorker(t *testing.T) {
	factory := common.Factory{}

	conn, err := factory.NewConnection()

	if err != nil {
		t.Error(err)
	}

	config, err := LoadConfiguration(nil)

	if err != nil {
		t.Error(err)
	}

	worker := NewWorker(conn, config)

	if err != nil {
		t.Error(err)
	}

	want := runtime.GOMAXPROCS(0)

	if cap(worker.taskQueue) != want {
		t.Errorf("NewWorker(%q) == %q, want %q", "taskQueue length", cap(worker.taskQueue), want)
	}
}

//TODO: Test
func TestHandleTask(t *testing.T) {
	common.RegisterTask("test", func() common.TaskHandler {
		return nil
	})
}
