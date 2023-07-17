package cmd

import (
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func rollEditsDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RollEditsRequestProto)
	return parseRequest(b, req)
}

func rollEdits(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RollEditsRequestProto)
	return opfsRollEdits(req)
}

func opfsRollEdits(r *hdfs.RollEditsRequestProto) (*hdfs.RollEditsResponseProto, error) {
	return &hdfs.RollEditsResponseProto {
		NewSegmentTxId: proto.Uint64(0),
	}, nil
}
