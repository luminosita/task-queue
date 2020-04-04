package util

import (
	"github.com/mnikita/task-queue/pkg/log"
	"sync"
	"time"
)

func WaitTimeout(wg *sync.WaitGroup, timeout time.Duration) error {
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-c:
		return nil // completed normally
	case <-time.After(timeout):
		return log.WorkerWaitTimeoutError(timeout) // timed out
	}
}
