package common

import (
	"time"
)

type ConsumerHandler interface {
	Reserve(timeout time.Duration) (id uint64, body []byte, err error)
	Release(id uint64, pri uint32, delay time.Duration) error
	Delete(id uint64) error
	Close() error
}
