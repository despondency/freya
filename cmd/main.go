package main

import (
	"flag"
	"fmt"
	"github.com/despondency/freya/internal/raft"
	"github.com/despondency/freya/internal/storage"
	"github.com/despondency/freya/internal/transport"
	"github.com/lni/dragonboat/v3/config"
	"github.com/lni/dragonboat/v3/logger"
	"strings"
)

func main() {
	nodeID := flag.Int("node-id", 1, "NodeID to use")
	clusterID := flag.Int("cluster-id", 1, "ClusterID to use")
	restPort := flag.Int("rest-port", 8080, "Rest port")
	members := flag.String("members", "localhost:63001,localhost:63002,localhost:63003", "Cluster Members")
	join := flag.Bool("join", false, "Joining a new node")
	flag.Parse()
	fmt.Printf("Freya started with the following args: \n")
	fmt.Printf("nodeID: %d\n", *nodeID)
	fmt.Printf("clusterID: %d\n", *clusterID)
	fmt.Printf("rest port: %d\n", *restPort)
	fmt.Printf("members: %s\n", *members)
	fmt.Printf("join: %v\n", *join)
	clusterMembers := make(map[uint64]string)
	splittedMembers := strings.Split(*members, ",")
	if !*join {
		for idx, v := range splittedMembers {
			clusterMembers[uint64(idx+1)] = v
		}
	}
	logger.GetLogger("raft").SetLevel(logger.ERROR)
	logger.GetLogger("rsm").SetLevel(logger.ERROR)
	logger.GetLogger("transport").SetLevel(logger.ERROR)
	logger.GetLogger("grpc").SetLevel(logger.ERROR)
	rc := &config.Config{
		NodeID:             uint64(*nodeID),
		ClusterID:          uint64(*clusterID),
		ElectionRTT:        15,
		HeartbeatRTT:       3,
		CheckQuorum:        true,
		SnapshotEntries:    10,
		CompactionOverhead: 5,
	}
	srv, err := raft.NewRaftServer("database", clusterMembers[uint64(*nodeID)], clusterMembers, *join, storage.NewBadgerStorage, rc)
	if err != nil {
		panic(err)
	}
	g := transport.NewGRPCServer(*restPort, srv)
	g.StartServer()
	//rest := transport.NewRestTransport(*restPort, srv)
	//rest.StartTransport()
}
