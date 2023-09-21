package cmd

import (
	"log"
	"context"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func isFileClosedDec(b []byte) (proto.Message, error) {
	req := new(hdfs.IsFileClosedRequestProto)
	return parseRequest(b, req)
}

func isFileClosed(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.IsFileClosedRequestProto)
	log.Printf("src %v\n", req.GetSrc())
	res, err := opfsIsFileClosed(ctx, req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsIsFileClosed(ctx context.Context, r *hdfs.IsFileClosedRequestProto) (*hdfs.IsFileClosedResponseProto, error) {
	return &hdfs.IsFileClosedResponseProto {
		Result: proto.Bool(true),
	}, nil
}
