package datanode

import (
	"context"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func evictWritersDec(b []byte) (proto.Message, error) {
	req := new(hdfs.EvictWritersRequestProto)
	return parseRequest(b, req)
}

func evictWriters(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.EvictWritersRequestProto)
	res, err := opfsEvictWriters(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}
//need to monitor all write operation
func opfsEvictWriters(r *hdfs.EvictWritersRequestProto) (*hdfs.EvictWritersResponseProto, error) {
	return new(hdfs.EvictWritersResponseProto), nil
}
