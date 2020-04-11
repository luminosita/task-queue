//Something

//+build wireinject

package cli

import (
	"github.com/google/wire"
	"github.com/mnikita/task-queue/pkg/beanstalkd"
	"github.com/mnikita/task-queue/pkg/container"
)

func InitializeCli(config *Configuration) *Cli {
	wire.Build(NewCli, container.WireSet, beanstalkd.WireSet)

	return &Cli{}
}
