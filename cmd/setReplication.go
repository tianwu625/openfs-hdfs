package cmd

import (
	"context"
	"log"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func setReplicationDec(b []byte) (proto.Message, error) {
	req := new(hdfs.SetReplicationRequestProto)
	return parseRequest(b, req)
}

func setReplication(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.SetReplicationRequestProto)
	log.Printf("src %v\nreplication %v\n", req.GetSrc(), req.GetReplication())
	return opfsSetReplication(req)
}

func opfsSetReplication(r *hdfs.SetReplicationRequestProto) (*hdfs.SetReplicationResponseProto, error) {
	src := r.GetSrc()
	reps := r.GetReplication()

	log.Printf("src %v reps %v", src, reps)

	res := &hdfs.SetReplicationResponseProto {
		Result: proto.Bool(true),
	}

	return res, nil
}
