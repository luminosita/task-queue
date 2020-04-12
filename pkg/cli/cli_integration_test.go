package cli

import (
	"github.com/mnikita/task-queue/pkg/util"
	"github.com/stretchr/testify/assert"
	"os"
	"syscall"
	"testing"
	"time"
)

type Mock struct {
	t *testing.T

	cli Handler
}

func newMock(t *testing.T) *Mock {
	m := &Mock{}
	m.t = t

	return m
}

func setupTest(m *Mock, config *Configuration) func() {
	if m == nil {
		panic("Mock not initialized")
	}

	m.cli = InitializeCli(config)

	assert.Nil(m.t, m.cli.Init())

	// Test teardown - return a closure for use by 'defer'
	return func() {
		defer util.AssertPanic(m.t)

		assert.Nil(m.t, m.cli.Close())
	}
}

func TestContainerStart(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	if err := os.Setenv(EnvUrl, "tcp://127.0.0.1:11300"); err != nil {
		panic(err)
	}

	config := &Configuration{}

	m := newMock(t)
	setupTest(m, config)

	var ch chan os.Signal

	go func() {
		err := m.cli.Start(func(c chan os.Signal) {
			ch = c
		})

		assert.Nil(t, err)
	}()

	time.Sleep(time.Second)
	ch <- syscall.SIGINT
	time.Sleep(time.Second)
}
