package cmd

import (
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getFsECBlockGroupStatsDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetFsECBlockGroupStatsRequestProto)
	return parseRequest(b, req)
}

func getFsECBlockGroupStats(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetFsECBlockGroupStatsRequestProto)
	return opfsGetFsECBlockGroupStats(req)
}

func opfsGetFsECBlockGroupStats(r *hdfs.GetFsECBlockGroupStatsRequestProto)(*hdfs.GetFsECBlockGroupStatsResponseProto, error) {
	return &hdfs.GetFsECBlockGroupStatsResponseProto {
		LowRedundancy: proto.Uint64(0),
		CorruptBlocks: proto.Uint64(0),
		MissingBlocks: proto.Uint64(0),
		BlocksInFuture: proto.Uint64(0),
		PendingDeletionBlocks: proto.Uint64(0),
		HighestPrioLowRedundancyBlocks:proto.Uint64(0),
	}, nil
}
