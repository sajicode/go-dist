package main

import (
	"fmt"

	"github.com/sajicode/go-dist-app/src/distributed/coordinator"
)

func main() {
	ql := coordinator.NewQueueListener()
	go ql.ListenForNewSource()

	var a string
	fmt.Scanln(&a)
}
