package util

import (
	"github.com/fsnotify/fsnotify"
	"github.com/mnikita/task-queue/pkg/log"
	"runtime"
	"sync"
	"time"
)

type ConfigWatcherEventHandler interface {
	OnConfigModified()
}

type ConfigWatcher struct {
	eventHandler ConfigWatcherEventHandler

	watcher *fsnotify.Watcher

	watchStarted bool
}

func NewConfigWatcher(eventHandler ConfigWatcherEventHandler) (c *ConfigWatcher, err error) {
	c = &ConfigWatcher{}

	c.eventHandler = eventHandler

	// creates a new file watcher
	c.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func GetSystemConcurrency() (concurrency int) {
	return runtime.GOMAXPROCS(0)
}

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

func (c *ConfigWatcher) WatchConfigFile(configFile string) (err error) {
	if !c.watchStarted {
		go func() {
			log.Logger().ConfigWatchStart()

			for {
				select {
				case event, ok := <-c.watcher.Events:
					if !ok {
						return
					}

					if event.Op&fsnotify.Write == fsnotify.Write {
						log.Logger().ConfigWatchModified(event.Name)

						c.eventHandler.OnConfigModified()
					}
				case err, ok := <-c.watcher.Errors:
					if !ok {
						return
					}

					log.Logger().ConfigWatchError(err)
				}
			}
		}()
	}

	if configFile != "" {
		err = c.watcher.Add(configFile)
		if err != nil {
			return err
		}

		log.Logger().ConfigWatchFile(configFile)
	}

	return nil
}

func (c *ConfigWatcher) StopWatch() (err error) {
	log.Logger().ConfigWatchStop()

	return c.watcher.Close()
}
