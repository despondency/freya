package transport

import (
	"context"
	"fmt"
	g "github.com/despondency/freya/grpc"
	"github.com/despondency/freya/internal/raft"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"net"
)

type GRPCServer struct {
	port       int
	raftServer *raft.Server
	g.UnimplementedKvServer
}

func (gs *GRPCServer) Get(ctx context.Context, req *g.GetRequest) (*g.GetResponse, error) {
	return gs.raftServer.Get(req)
}

func (gs *GRPCServer) Put(ctx context.Context, req *g.PutRequest) (*emptypb.Empty, error) {
	err := gs.raftServer.Put(req)
	return &emptypb.Empty{}, err
}

func NewGRPCServer(port int, raftServer *raft.Server) *GRPCServer {
	return &GRPCServer{
		port:       port,
		raftServer: raftServer,
	}
}

func (gs *GRPCServer) StartServer() {
	s := grpc.NewServer()
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", gs.port))
	if err != nil {
		log.Fatalf("Error occured trying to open tcp connection %v", err)
	}
	g.RegisterKvServer(s, gs)
	log.Fatalln(s.Serve(lis))
}
