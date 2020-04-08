package util

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"runtime/debug"
	"testing"
)

func AssertFlags(t *testing.T, flags reflect.Value) {
	e := flags.Elem()

	for i := 0; i < e.NumField(); i++ {
		varName := e.Type().Field(i).Name
		varValue := e.Field(i).Interface()

		assert.Equal(t, 0, varValue, varName)
	}
}

func AssertPanic(t *testing.T) {
	if r := recover(); r != nil {
		t.Errorf("PANIC %+v\n%s", r, string(debug.Stack()))
	}
}
