package tpc

import (
	"fmt"
	"io"
	. "launchpad.net/gocheck"
	"log"
	"os"
	"os/exec"
	"strconv"
	"sync"
	"time"
)

const ReplicaCount = 4

var masterCmd *exec.Cmd

func startMaster(t *C) {
	masterCmd = startCmd(t, "src.exe", "-m", "-n", strconv.Itoa(ReplicaCount))

	client := NewMasterClient(MasterPort)

	verify(t,
		func() bool {
			_, err := client.Ping("whatever")
			return err == nil
		},
		"Ping to Master successful.",
		"Unable to Ping after running Master.")
}

var replicas = [ReplicaCount]*exec.Cmd{}

func startReplicas(c *C, shouldRestart bool) {
	var wg sync.WaitGroup
	for i := 0; i < ReplicaCount; i++ {
		wg.Add(1)
		go func(i int) {
			startReplica(c, i, shouldRestart)
			wg.Done()
		}(i)
	}
	wg.Wait()
}

func startReplica(c *C, n int, shouldRestart bool) {
	replicas[n] = startCmd(c, "src.exe", "-r", "-i", strconv.Itoa(n))

	client := NewReplicaClient(GetReplicaHost(n))

	if shouldRestart {
		go func(cmd *exec.Cmd) {
			if cmd != nil {
				cmd.Wait()
				if replicas[n] != nil {
					startReplica(c, n, shouldRestart)
				}
			}
		}(replicas[n])
	}

	verify(c,
		func() bool {
			_, err := client.Ping("whatever")
			return err == nil
		},
		fmt.Sprintf("Ping to Replica %v successful.", n),
		fmt.Sprintf("Unable to Ping after running Replica %v.", n))
}

func killMaster(c *C) {
	if masterCmd == nil {
		return
	}

	masterCmd.Process.Kill()
	masterCmd = nil
	client := NewMasterClient(MasterPort)

	verify(c,
		func() bool {
			_, err := client.Ping("whatever")
			return err != nil
		},
		"Master killed successfully.",
		"Able to Ping after killing Master.")

}

func verify(c *C, check func() bool, successMessage string, failMessage string) {
	for i := 0; i < 1000; i++ {
		if check() {
			log.Println(successMessage)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.Fatal(failMessage)
}

func killReplica(c *C, i int) {
	replicaCmd := replicas[i]
	if replicaCmd == nil {
		return
	}

	replicas[i] = nil
	replicaCmd.Process.Kill()

	client := NewReplicaClient(GetReplicaHost(i))
	verify(c,
		func() bool {
			_, err := client.Ping("whatever")
			return err != nil
		},
		fmt.Sprintf("Replica %v killed successfully.", i),
		fmt.Sprintf("Able to Ping after killing Replica %v.", i))

}

func killAll(c *C) {
	var wg sync.WaitGroup
	wg.Add(1 + ReplicaCount)

	go func() {
		killMaster(c)
		wg.Done()
	}()

	for i, _ := range replicas {
		go func(c *C, i int) {
			killReplica(c, i)
			wg.Done()
		}(c, i)
	}
	wg.Wait()
}

func startCmd(c *C, path string, args ...string) *exec.Cmd {
	cmd := exec.Command(path, args...)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		c.Fatal(err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		c.Fatal(err)
	}
	err = cmd.Start()
	if err != nil {
		c.Fatal(err)
	}

	go io.Copy(os.Stdout, stdout)
	go io.Copy(os.Stderr, stderr)

	return cmd
}
