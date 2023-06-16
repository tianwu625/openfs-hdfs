package cmd

import (
	"log"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)


func completeDec(b []byte) (proto.Message, error) {
	req := new(hdfs.CompleteRequestProto)
	return parseRequest(b, req)
}

func complete(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.CompleteRequestProto)
	log.Printf("src %v\nclientName %v\nLast %v\nFileId %v\n", req.GetSrc(), req.GetClientName(), req.GetLast(), req.GetFileId())
	return opfsComplete(req)
}

func opfsComplete(r *hdfs.CompleteRequestProto) (*hdfs.CompleteResponseProto, error) {
	res := new(hdfs.CompleteResponseProto)
	res.Result = proto.Bool(true)

	return res, nil
}
