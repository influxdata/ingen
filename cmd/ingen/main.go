package main

import (
	"fmt"
	"os"

	"github.com/influxdata/ingen/cmd"
	"github.com/influxdata/ingen/cmd/ingen/oss"
)

func main() {
	var err error
	name, args := cmd.ParseCommandName(os.Args[1:])
	switch name {
	case "oss":
		cl := oss.New()
		err = cl.Run(args)

	default:
		err = fmt.Errorf("invalid command: %s", name)
	}

	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
