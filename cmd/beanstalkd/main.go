package main

import (
	"github.com/mnikita/task-queue/pkg/log"
	"os"
	"sort"

	beantq "github.com/mnikita/task-queue/pkg/beanstalk"
	"github.com/urfave/cli/v2"
)

func main() {
	var configFile string
	var tubes cli.StringSlice
	var urlText string

	app := &cli.App{
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "config",
				Aliases:     []string{"c"},
				Usage:       "Load configuration from `FILE`",
				Value:       "config.json",
				Destination: &configFile,
			},
			&cli.StringSliceFlag{
				Name:        "tubes",
				Aliases:     []string{"t"},
				Usage:       "Specify Beanstalkd tubes to watch",
				Destination: &tubes,
			},
			&cli.StringFlag{
				Name:        "url",
				Aliases:     []string{"u"},
				Usage:       "Specify Beanstalkd server URL",
				Value:       "tcp://127.0.0.1:11300",
				Destination: &urlText,
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "start",
				Usage: "starts Beanstalkd Consumer service",
				Action: func(c *cli.Context) error {
					var config = &beantq.Configuration{
						Tubes:      tubes.Value(),
						Url:        urlText,
						ConfigFile: configFile,
					}

					return beantq.Start(config)
				},
			},
			{
				Name:  "put",
				Usage: "registers job on Beanstalkd Consumer service",
				Action: func(c *cli.Context) error {
					var config = &beantq.Configuration{
						Tubes:      tubes.Value(),
						Url:        urlText,
						ConfigFile: configFile,
					}

					return beantq.Put(config)
				},
			},
			{
				Name:  "default-config",
				Usage: "writes default configuration",
				Action: func(c *cli.Context) error {
					return beantq.WriteDefaultConfiguration(configFile)
				},
			},
		},
		Name: "Beanstalkd Consumer Service",
		Description: "Consumer is a polling service for Beanstalkd task queue. " +
			"Provides platform for registering diverse Go task handlers used to process scheduled tasks.",
		Usage:    "Emisia",
		HelpName: "consumer",
	}

	sort.Sort(cli.FlagsByName(app.Flags))
	sort.Sort(cli.CommandsByName(app.Commands))

	err := app.Run(os.Args)
	if err != nil {
		log.Logger().Fatal(err)
	}
}

//var conn, _ = beanstalk.Dial("tcp", "127.0.0.1:11300")
//
//func Example_reserve() {
//	id, body, err := conn.Reserve(5 * time.Second)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("job", id)
//	fmt.Println(string(body))
//}
//
//func Example_reserveOtherTubeSet() {
//	tubeSet := beanstalk.NewTubeSet(conn, "mytube1", "mytube2")
//	id, body, err := tubeSet.Reserve(10 * time.Hour)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("job", id)
//	fmt.Println(string(body))
//}
//
//func Example_put() {
//	id, err := conn.Put([]byte("myjob"), 1, 0, time.Minute)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("job", id)
//}
//
//func Example_putOtherTube() {
//	tube := &beanstalk.Tube{Conn: conn, Name: "mytube"}
//	id, err := tube.Put([]byte("myjob"), 1, 0, time.Minute)
//	if err != nil {
//		panic(err)
//	}
//	fmt.Println("job", id)
//}
