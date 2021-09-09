package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ianobermiller/gotwopc/tpc"
)

func main() {

	start := time.Now()
	sigs := make(chan os.Signal, 1)
	quitting := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)

	go func() {
		_ = <-sigs
		fmt.Printf("Total time taken: %d\n", time.Since(start).Nanoseconds())
		quitting <- true
		_ = <-quitting
		os.Exit(0)
	}()

	if _, set := os.LookupEnv("CLIENT"); set {
		client := tpc.NewMasterClient(tpc.MasterPort)

		reqs, _ := os.LookupEnv("REQUESTS")
		requests, _ := strconv.Atoi(reqs)

		for i := 0; i < requests; i++ {
			client.Put("a"+strconv.Itoa(i), "b"+strconv.Itoa(i))
		}
		fmt.Printf("Total time taken: %d\n", time.Since(start).Nanoseconds())
	} else {
		tpc.Start(quitting)
	}
}
