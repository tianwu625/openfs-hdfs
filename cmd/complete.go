package cmd

import (
	"log"
	"context"
	"errors"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"github.com/openfs/openfs-hdfs/internal/opfsBlocksMap"
	"google.golang.org/protobuf/proto"
)


func completeDec(b []byte) (proto.Message, error) {
	req := new(hdfs.CompleteRequestProto)
	return parseRequest(b, req)
}

func complete(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.CompleteRequestProto)
	log.Printf("src %v\nclientName %v\nLast %v\nFileId %v\n", req.GetSrc(), req.GetClientName(), req.GetLast(), req.GetFileId())
	return opfsComplete(req)
}

func opfsComplete(r *hdfs.CompleteRequestProto) (*hdfs.CompleteResponseProto, error) {
	res := &hdfs.CompleteResponseProto {
		Result: proto.Bool(false),
	}
	blast := convertProtoToBlockMap(r.GetLast())
	bsm := getBlocksMap()
	if err := bsm.CompleteFile(r.GetSrc(), r.GetClientName(), blast, r.GetFileId()); err != nil {
		if errors.Is(err, opfsBlocksMap.ErrNotCommited) {
			log.Printf("not Commited block will return false in reply")
			return res, nil
		}
		return nil, err
	}

	res.Result = proto.Bool(true)

	return res, nil
}
