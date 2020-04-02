package common

type TaskHandler interface {
	Handle() error
}

type Task struct {
	Name    string
	Payload []byte
}

type TaskConstructor func() TaskHandler
