package datanode

import (
	"context"
	"log"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func shutdownDatanodeDec(b []byte) (proto.Message, error) {
	req := new(hdfs.ShutdownDatanodeRequestProto)
	return parseRequest(b, req)
}

func shutdownDatanode(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.ShutdownDatanodeRequestProto)
	res, err := opfsShutdownDatanode(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsRestartDatanode() error {
	log.Printf("restart datanode")

	return nil
}

func opfsStopDatanode() error {
	log.Printf("stop datanode")
	return nil
}

func opfsShutdownDatanode(r *hdfs.ShutdownDatanodeRequestProto) (*hdfs.ShutdownDatanodeResponseProto, error) {
	upgrade := r.GetForUpgrade()

	if upgrade {
		if err := opfsRestartDatanode(); err != nil {
			return nil, err
		}
		return new(hdfs.ShutdownDatanodeResponseProto), nil
	}
	if err := opfsStopDatanode(); err != nil {
		return nil, err
	}

	return new(hdfs.ShutdownDatanodeResponseProto), nil
}

