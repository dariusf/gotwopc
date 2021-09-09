package main

import (
	"os"
	"strconv"

	"github.com/ianobermiller/gotwopc/tpc"
)

func main() {

	if _, set := os.LookupEnv("CLIENT"); set {
		client := tpc.NewMasterClient(tpc.MasterPort)

		for i := 0; i < 100; i++ {
			client.Put("a" + strconv.Itoa(i), "b" + strconv.Itoa(i))
		}
		println("done")
	} else {
		tpc.Start()
	}
}
