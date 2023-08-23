package cmd

import (
	"context"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func refreshNodesDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RefreshNodesRequestProto)
	return parseRequest(b, req)
}

func refreshNodes(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RefreshNodesRequestProto)
	return opfsRefreshNodes(req)
}

func opfsRefreshNodes(r *hdfs.RefreshNodesRequestProto) (*hdfs.RefreshNodesResponseProto, error) {

	return &hdfs.RefreshNodesResponseProto{}, nil
}

