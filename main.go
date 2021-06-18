package main

import (
	"os"

	"github.com/ianobermiller/gotwopc/tpc"
)

func main() {

	if _, set := os.LookupEnv("CLIENT"); set {
		client := tpc.NewMasterClient(tpc.MasterPort)
		client.Put("a", "b")
	} else {
		tpc.Start()
	}
}
