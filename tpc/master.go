package tpc

import (
	"errors"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/dchest/uniuri"
	"github.com/ianobermiller/gotwopc/rvc"
)

var (
	TxAbortedError = errors.New("Transaction aborted.")
)

type Master struct {
	replicaCount int
	replicas     []*ReplicaClient
	log          *logger
	txs          map[string]TxState
	didSuicide   bool
	monitor      *rvc.Monitor
	committed    []int
	aborted      []int
}

type PutArgs struct {
	Key   string
	Value string
}

type PutTestArgs struct {
	Key           string
	Value         string
	MasterDeath   MasterDeath
	ReplicaDeaths []ReplicaDeath
}

type GetArgs struct {
	Key string
}

type GetTestArgs struct {
	Key        string
	ReplicaNum int
}

type DelArgs struct {
	Key string
}

type DelTestArgs struct {
	Key           string
	MasterDeath   MasterDeath
	ReplicaDeaths []ReplicaDeath
}

type StatusArgs struct {
	TxId string
}

type StatusResult struct {
	State TxState
}

type PingArgs struct {
	Key string
}

type GetResult struct {
	Value string
}

func NewMaster(replicaCount int, quitting chan bool) *Master {
	l := newLogger("logs/master.txt")
	replicas := make([]*ReplicaClient, replicaCount)
	mParts := map[string]bool{}
	for i := 0; i < replicaCount; i++ {
		replicas[i] = NewReplicaClient(GetReplicaHost(i))
		mParts[strconv.Itoa(i)] = true
	}
	monitor := rvc.NewMonitor(map[string]map[string]bool{"P": mParts})
	go func() {
		_ = <-quitting
		monitor.PrintLog()
		quitting <- true
	}()
	return &Master{replicaCount, replicas, l, make(map[string]TxState), false, monitor, []int{}, []int{}}
}

func (m *Master) Get(args *GetArgs, reply *GetResult) (err error) {
	return m.GetTest(&GetTestArgs{args.Key, -1}, reply)
}

func (m *Master) GetTest(args *GetTestArgs, reply *GetResult) (err error) {
	log.Println("Master.Get is being called")
	rn := args.ReplicaNum
	if rn < 0 {
		rn = rand.Intn(m.replicaCount)
	}
	r, err := m.replicas[rn].Get(args.Key)
	if err != nil {
		log.Printf("Master.Get: request to replica %v for key %v failed\n", rn, args.Key)
		return
	}
	reply.Value = *r
	return nil
}

func (m *Master) Del(args *DelArgs, _ *int) (err error) {
	var i int
	return m.DelTest(&DelTestArgs{args.Key, MasterDontDie, make([]ReplicaDeath, m.replicaCount)}, &i)
}

func (m *Master) DelTest(args *DelTestArgs, _ *int) (err error) {
	return m.mutate(
		DelOp,
		args.Key,
		args.MasterDeath,
		args.ReplicaDeaths,
		func(r *ReplicaClient, txId string, i int, rd ReplicaDeath) (*bool, error) {
			return r.TryDel(args.Key, txId, rd)
		})
}

func (m *Master) Put(args *PutArgs, _ *int) (err error) {
	var i int
	return m.PutTest(&PutTestArgs{args.Key, args.Value, MasterDontDie, make([]ReplicaDeath, m.replicaCount)}, &i)
}

func (m *Master) PutTest(args *PutTestArgs, _ *int) (err error) {
	return m.mutate(
		PutOp,
		args.Key,
		args.MasterDeath,
		args.ReplicaDeaths,
		func(r *ReplicaClient, txId string, i int, rd ReplicaDeath) (*bool, error) {
			return r.TryPut(args.Key, args.Value, txId, rd)
		})
}

func getReplicaDeath(replicaDeaths []ReplicaDeath, n int) ReplicaDeath {
	rd := ReplicaDontDie
	if replicaDeaths != nil && len(replicaDeaths) > n {
		rd = replicaDeaths[n]
	}
	return rd
}

func (m *Master) abstract(shouldAbort bool) rvc.Global {
	c := map[string]bool{}
	for _, v := range m.committed {
		c[strconv.Itoa(v)] = true
	}
	a := map[string]bool{}
	for _, v := range m.aborted {
		a[strconv.Itoa(v)] = true
	}
	return rvc.Global{HasAborted: shouldAbort, Committed: c, Aborted: a}
}

func (m *Master) mutate(operation Operation, key string, masterDeath MasterDeath, replicaDeaths []ReplicaDeath, f func(r *ReplicaClient, txId string, i int, rd ReplicaDeath) (*bool, error)) (err error) {
	action := operation.String()
	txId := uniuri.New()
	m.log.writeState(txId, Started)
	m.txs[txId] = Started

	// Send out all mutate requests in parallel. If any abort, send on the channel.
	// Channel must be buffered to allow the non-blocking read in the switch.
	shouldAbort := make(chan int, m.replicaCount)
	log.Println("Master."+action+" asking replicas to "+action+" tx:", txId, "key:", key)
	m.forEachReplica(func(i int, r *ReplicaClient) {
		pid := strconv.Itoa(i)
		success, err := f(r, txId, i, getReplicaDeath(replicaDeaths, i))
		if err := m.monitor.Step(m.abstract(false), rvc.CSendPrepare8, pid); err != nil {
			log.Printf("%v\n", err)
		}
		if err != nil {
			log.Println("Master."+action+" r.Try"+action+":", err)
		}
		if success == nil || !*success {
			if err := m.monitor.Step(m.abstract(false), rvc.CReceiveAbort10, pid); err != nil {
				log.Printf("%v\n", err)
			}
			shouldAbort <- 1
		} else {
			if err := m.monitor.Step(m.abstract(false), rvc.CReceivePrepared9, pid); err != nil {
				log.Printf("%v\n", err)
			}
		}
	})

	// If at least one replica needed to abort
	select {
	case <-shouldAbort:
		log.Println("Master."+action+" asking replicas to abort tx:", txId, "key:", key)
		m.log.writeState(txId, Aborted)
		m.txs[txId] = Aborted
		m.sendAbort(action, txId)
		return TxAbortedError
	default:
		break
	}

	// The transaction is now officially committed
	m.dieIf(masterDeath, MasterDieBeforeLoggingCommitted)
	m.log.writeState(txId, Committed)
	m.dieIf(masterDeath, MasterDieAfterLoggingCommitted)
	m.txs[txId] = Committed

	log.Println("Master."+action+" asking replicas to commit tx:", txId, "key:", key)
	m.sendAndWaitForCommit(action, txId, replicaDeaths)
	log.Println("commit ok")

	return
}

func (m *Master) sendAbort(action string, txId string) {
	m.forEachReplica(func(i int, r *ReplicaClient) {
		pid := strconv.Itoa(i)
		_, err := r.Abort(txId)
		if err := m.monitor.Step(m.abstract(true), rvc.CSendAbort13, pid); err != nil {
			log.Printf("%v\n", err)
		}
		if err != nil {
			m.aborted = append(m.aborted, i)
			if err := m.monitor.Step(m.abstract(true), rvc.CReceiveAbortAck14, pid); err != nil {
				log.Printf("%v\n", err)
			}
			log.Println("Master."+action+" r.Abort:", err)
		}
	})
}

func (m *Master) sendAndWaitForCommit(action string, txId string, replicaDeaths []ReplicaDeath) {
	m.forEachReplica(func(i int, r *ReplicaClient) {
		for {
			pid := strconv.Itoa(i)
			_, err := r.Commit(txId, getReplicaDeath(replicaDeaths, i))
			if err := m.monitor.Step(m.abstract(false), rvc.CSendCommit11, pid); err != nil {
				log.Printf("%v\n", err)
			}
			if err == nil {
				m.committed = append(m.committed, i)
				if err := m.monitor.Step(m.abstract(false), rvc.CReceiveCommitAck12, pid); err != nil {
					log.Printf("%v\n", err)
				}
				break
			}
			log.Println("Master."+action+" r.Commit:", err)
			time.Sleep(100 * time.Millisecond)
		}
	})
}

func (m *Master) forEachReplica(f func(i int, r *ReplicaClient)) {
	var wg sync.WaitGroup
	wg.Add(m.replicaCount)
	for i := 0; i < m.replicaCount; i++ {
		go func(i int, r *ReplicaClient) {
			defer wg.Done()
			f(i, r)
		}(i, m.replicas[i])
	}
	wg.Wait()
}

func (m *Master) Ping(args *PingArgs, reply *GetResult) (err error) {
	reply.Value = args.Key
	return nil
}

func (m *Master) Status(args *StatusArgs, reply *StatusResult) (err error) {
	state, ok := m.txs[args.TxId]
	if !ok {
		state = NoState
	}
	reply.State = state
	return nil
}

func (m *Master) recover() (err error) {
	entries, err := m.log.read()
	if err != nil {
		return
	}

	m.didSuicide = false
	for _, entry := range entries {
		switch entry.txId {
		case killedSelfMarker:
			m.didSuicide = true
			continue
		case firstRestartAfterSuicideMarker:
			m.didSuicide = false
			continue
		}

		m.txs[entry.txId] = entry.state
	}

	for txId, state := range m.txs {
		switch state {
		case Started:
			fallthrough
		case Aborted:
			log.Println("Aborting tx", txId, "during recovery.")
			m.sendAbort("recover", txId)
		case Committed:
			log.Println("Committing tx", txId, "during recovery.")
			m.sendAndWaitForCommit("recover", txId, make([]ReplicaDeath, m.replicaCount))
		}
	}

	if m.didSuicide {
		m.log.writeSpecial(firstRestartAfterSuicideMarker)
	}
	return
}

func (m *Master) dieIf(actual MasterDeath, expected MasterDeath) {
	if !m.didSuicide && actual == expected {
		log.Println("Killing self as requested at", expected)
		m.log.writeSpecial(killedSelfMarker)
		os.Exit(1)
	}
}

func runMaster(replicaCount int, quitting chan bool) {
	if replicaCount <= 0 {
		log.Fatalln("Replica count must be greater than 0.")
	}

	master := NewMaster(replicaCount, quitting)
	err := master.recover()
	if err != nil {
		log.Fatal("Error during recovery: ", err)
	}

	server := rpc.NewServer()
	server.Register(master)
	log.Println("Master listening on port", MasterPort)
	http.ListenAndServe(MasterPort, server)
}
