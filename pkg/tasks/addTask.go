package tasks

import (
	"log"
)

type AddTask struct {
	A int
	B int
}

func (task *AddTask) Handle() error {
	log.Printf("AddTask HANDLER, Result: %d", task.A+task.B)

	return nil
}
