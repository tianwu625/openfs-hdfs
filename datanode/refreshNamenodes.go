package datanode

import (
	"context"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func refreshNamenodesDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RefreshNamenodesRequestProto)
	return parseRequest(b, req)
}

func refreshNamenodes(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RefreshNamenodesRequestProto)
	res, err := opfsRefreshNamenodes(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsRefreshNamenodes(r *hdfs.RefreshNamenodesRequestProto) (*hdfs.RefreshNamenodesResponseProto, error) {

	return new(hdfs.RefreshNamenodesResponseProto), nil
}
