package cmd

import (
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getFsReplicatedBlockStatsDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetFsReplicatedBlockStatsRequestProto)
	return parseRequest(b, req)
}

func getFsReplicatedBlockStats(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetFsReplicatedBlockStatsRequestProto)
	return opfsGetFsReplicatedBlockStats(req)
}

func opfsGetFsReplicatedBlockStats(r *hdfs.GetFsReplicatedBlockStatsRequestProto) (*hdfs.GetFsReplicatedBlockStatsResponseProto, error) {
	return &hdfs.GetFsReplicatedBlockStatsResponseProto {
		LowRedundancy: proto.Uint64(0),
		CorruptBlocks: proto.Uint64(0),
		MissingBlocks: proto.Uint64(0),
		MissingReplOneBlocks: proto.Uint64(0),
		BlocksInFuture: proto.Uint64(0),
		PendingDeletionBlocks: proto.Uint64(0),
		HighestPrioLowRedundancyBlocks: proto.Uint64(0),
	}, nil
}
