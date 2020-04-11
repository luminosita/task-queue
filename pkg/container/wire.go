//Something

//+build wireinject

package container

import (
	"github.com/google/wire"
	"github.com/mnikita/task-queue/pkg/beanstalkd"
)

func InitializeContainer() *Container {
	wire.Build(WireSet, beanstalkd.WireSet)

	return &Container{}
}
