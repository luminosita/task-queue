package cli_test

import (
	"encoding/json"
	"github.com/mnikita/task-queue/pkg/cli"
	"github.com/mnikita/task-queue/pkg/log"
	"github.com/mnikita/task-queue/pkg/util"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"syscall"
	"testing"
	"time"
)

type Mock2 struct {
	t *testing.T

	cli cli.Handler
}

func newMock2(t *testing.T) *Mock2 {
	m := &Mock2{}
	m.t = t

	return m
}

func setupTest2(m *Mock2, config *cli.Configuration) func() {
	if m == nil {
		panic("Mock not initialized")
	}

	m.cli = cli.InitializeCli(config)

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

	//test with ENV url
	if err := os.Setenv(cli.EnvUrl, "tcp://127.0.0.1:11300"); err != nil {
		panic(err)
	}
	//put default tube to start beanstalkd.Tube for Put command
	if err := os.Setenv(cli.EnvTubes, "default"); err != nil {
		panic(err)
	}

	config := cli.NewConfiguration()

	m := newMock2(t)
	setupTest2(m, config)

	var ch chan os.Signal

	log.Logger().Info("TEST: Starting server ...")

	go func() {
		err := m.cli.Start(func(c chan os.Signal) {
			ch = c
		})

		assert.Nil(t, err)
	}()

	//wait for server to boot
	time.Sleep(time.Second)

	log.Logger().Info("TEST: Reading test data ...")

	td, err := readTestData()

	if err != nil {
		panic(err)
	}

	log.Logger().Info("TEST: Putting new tasks ...")

	var id uint64

	for _, tds := range td {
		id, err = m.cli.Put(tds)

		assert.True(t, id > 0)
		assert.Nil(t, err)
	}

	log.Logger().Info("TEST: Waiting before shutdown ...")
	//wait for all tasks to finish
	time.Sleep(time.Second * 10)

	log.Logger().Info("TEST: Deleting after test ...")

	//deleting last job. It is error job and should be in Buried state
	err = m.cli.Delete(id)
	assert.Nil(t, err)

	log.Logger().Info("TEST: Sending shutdown signal ...")

	ch <- syscall.SIGINT
	time.Sleep(time.Second)

	log.Logger().Info("TEST: Done.")
}

func readTestData() (testdata []json.RawMessage, err error) {
	//TODO: read from test volume or test path (Docker and local testing)
	bytes, err := ioutil.ReadFile("../test/testdata/integration_test_tasks.json")

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(bytes, &testdata)

	return testdata, err
}
