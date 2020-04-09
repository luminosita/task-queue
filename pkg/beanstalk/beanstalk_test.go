package beanstalk

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

type Mock struct {
	t *testing.T
}

func newMock(t *testing.T) (m *Mock) {
	m = &Mock{}
	m.t = t

	return
}

func setupTest(m *Mock) func() {
	if m == nil {
		panic("Mock not initialized")
	}

	return func() {
	}
}

func TestMain(m *testing.M) {
	//	log.Logger().Level = logrus.TraceLevel

	os.Exit(m.Run())
}

//TODO: Implement mock Beanstalkd connection for testing
func TestPut(t *testing.T) {
	t.SkipNow()
	defer setupTest(newMock(t))()

	var config = &Configuration{
		Tubes: []string{"default"},
		Url:   "mock",
	}

	err := Put(config)

	assert.NotNil(t, err)
}

//TODO: Implement mock io.Writer for testing
func TestWriteDefaultConfiguration(t *testing.T) {
	t.SkipNow()
	defer setupTest(newMock(t))()

	err := WriteDefaultConfiguration("")

	assert.NotNil(t, err)
}

//TODO: Implement mock Beanstalkd connection for testing
func TestStart(t *testing.T) {
	t.SkipNow()
	defer setupTest(newMock(t))()

	var config = &Configuration{
		Tubes: []string{"default"},
		Url:   "mock",
	}

	err := Start(config)

	assert.NotNil(t, err)
}
