package workers

import (
	"github.com/mnikita/task-queue/common"
	"runtime"
	"testing"
)

func TestLoadConfiguration(t *testing.T) {
	got, err := loadConfiguration()

	if err != nil {
		t.Error(err)
	}

	if got.concurrency != 0 {
		t.Errorf("LoadConfiguration(%q) == %q, want %q", "concurrency", got.concurrency, 0)
	}
}

func TestNewWorker(t *testing.T) {
	factory := common.Factory{}

	conn, err := factory.NewConnection()

	if err != nil {
		t.Error(err)
	}

	worker, err := NewWorker(conn)

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
}
