package cmd

import (
	"log"
	"context"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/logger"
	"github.com/openfs/openfs-hdfs/internal/opfs"
)

func recoverLeaseDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RecoverLeaseRequestProto)
	return parseRequest(b, req)
}

func recoverLease(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RecoverLeaseRequestProto)
	log.Printf("src %v\nclientName %v\n", req.GetSrc(), req.GetClientName())
	res, err := opfsRecoverLease(ctx, req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsRecoverLease(ctx context.Context, r *hdfs.RecoverLeaseRequestProto) (*hdfs.RecoverLeaseResponseProto,error) {
	src := r.GetSrc()
	//client := r.GetClientName()
	res := &hdfs.RecoverLeaseResponseProto {
		Result: proto.Bool(false),
	}
	f, err := opfs.Open(src)
	if err != nil {
		logger.LogIf(ctx, err)
		return res, nil
	}
	defer f.Close()

	return &hdfs.RecoverLeaseResponseProto {
		Result: proto.Bool(true),
	}, nil
}
