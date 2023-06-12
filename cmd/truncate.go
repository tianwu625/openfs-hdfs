package cmd

import (
	"log"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func truncateDec(b []byte) (proto.Message, error) {
	req := new(hdfs.TruncateRequestProto)
	return parseRequest(b, req)
}

func truncate(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.TruncateRequestProto)
	log.Printf("src %v\nnewLength %v\nclient name %v\n", req.GetSrc(), req.GetNewLength(), req.GetClientName())
	return opfsTruncate(req)
}

func opfsTruncate(r *hdfs.TruncateRequestProto) (*hdfs.TruncateResponseProto, error) {
	src := r.GetSrc()
	size := int64(r.GetNewLength())
	client := r.GetClientName()
	log.Printf("client name %v\n", client)
	res := new(hdfs.TruncateResponseProto)
	res.Result = proto.Bool(false)

	f, err := opfs.Open(src)
	if err != nil {
		log.Printf("truncate fail %v\n", err)
		return res, err
	}
	defer f.Close()
	err = f.Truncate(size)
	if err != nil {
		return res, err
	}
	res.Result = proto.Bool(true)

	return res, nil
}
