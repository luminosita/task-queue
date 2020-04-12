//Something

//+build wireinject

package cli

import (
	"github.com/google/wire"
	"github.com/mnikita/task-queue/pkg/beanstalkd"
	"github.com/mnikita/task-queue/pkg/container"
)

//TODO: Initialize with url, tubes to dispatch to connection.NewConfiguration
func InitializeCli(config *Configuration) *Cli {
	wire.Build(NewCli, container.WireSet, beanstalkd.WireSet)

	return &Cli{}
}
