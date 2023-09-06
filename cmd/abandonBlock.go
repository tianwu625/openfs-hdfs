package cmd

import (
	"log"
	"context"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func abandonBlockDec(b []byte) (proto.Message, error) {
	req := new(hdfs.AbandonBlockRequestProto)
	return parseRequest(b, req)
}

func abandonBlock(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.AbandonBlockRequestProto)
	log.Printf("req %v", req)

	res, err := opfsAbandonBlock(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsAbandonBlock(r *hdfs.AbandonBlockRequestProto)(*hdfs.AbandonBlockResponseProto, error) {
	b := r.GetB()
	log.Printf("b %v", b)
	src := r.GetSrc()
	log.Printf("src %v", src)
	holder := r.GetHolder()
	log.Printf("holder??? %v", holder)
	id := r.GetFileId()
	log.Printf("fileid %v", id)
	return new(hdfs.AbandonBlockResponseProto), nil
}
