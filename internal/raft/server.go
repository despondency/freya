package raft

import (
	"context"
	"errors"
	"fmt"
	"github.com/despondency/freya/grpc"
	"github.com/despondency/freya/internal/dto"
	"github.com/golang/protobuf/proto"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/config"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"path/filepath"
	"time"
)

type Server struct {
	nodeHost    *dragonboat.NodeHost
	nodeHostCfg *config.NodeHostConfig
	cfg         *config.Config
	members     map[uint64]string
	join        bool
	dataDir     string
	storageFunc sm.CreateOnDiskStateMachineFunc
}

func NewRaftServer(dataDir string, nodeHost string, members map[uint64]string, join bool, storageCreateFunc sm.CreateOnDiskStateMachineFunc, cfg *config.Config) (*Server, error) {
	dir := filepath.Join(dataDir, "raft", fmt.Sprintf("node%d", cfg.NodeID))
	if len(members) == 0 {
		return nil, errors.New("members are empty")
	}
	if storageCreateFunc == nil {
		return nil, errors.New("storage func is nil")
	}
	if cfg == nil {
		return nil, errors.New("cfg is nil")
	}
	if nodeHost == "" {
		return nil, errors.New("node host is empty")
	}
	nhc := &config.NodeHostConfig{
		WALDir:         dir,
		NodeHostDir:    dir,
		RTTMillisecond: 200,
		RaftAddress:    nodeHost,
	}
	nh, err := dragonboat.NewNodeHost(*nhc)
	if err != nil {
		return nil, err
	}
	if err = nh.StartOnDiskCluster(members, join, storageCreateFunc, *cfg); err != nil {
		return nil, err
	}
	return &Server{
		cfg:         cfg,
		nodeHostCfg: nhc,
		nodeHost:    nh,
		dataDir:     dir,
		members:     members,
		join:        join,
		storageFunc: storageCreateFunc,
	}, nil
}

func (s *Server) Get(req *grpc.GetRequest) (*grpc.GetResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res := make(dto.StringSlice, 0)
	for _, key := range req.Keys {
		result, err := s.nodeHost.SyncRead(ctx, s.cfg.ClusterID, []byte(key))
		if err != nil {
			return nil, err
		} else {
			r, ok := result.([]byte)
			if !ok {
				return nil, errors.New("not a byte array")
			}
			if len(r) > 0 {
				res = append(res, string(r))
			} else {
				res = append(res, "")
			}
		}
	}
	return &grpc.GetResponse{
		Values: res,
	}, nil
}

func (s *Server) Put(req *grpc.PutRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	//data, err := gojay.MarshalJSONObject(req)
	//if err != nil {
	//	panic(err)
	//}
	b, err := proto.Marshal(req)
	if err != nil {
		return err
	}
	cs := s.nodeHost.GetNoOPSession(s.cfg.ClusterID)
	_, err = s.nodeHost.SyncPropose(ctx, cs, b)
	if err != nil {
		return err
	}
	return nil
}
