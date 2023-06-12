package cmd

import (
	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getFsStatsDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetFsStatusRequestProto)
	return parseRequest(b, req)
}

func getFsStats(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetFsStatusRequestProto)
	return opfsGetFsStats(req)
}

func opfsGetFsStats(r *hdfs.GetFsStatusRequestProto) (*hdfs.GetFsStatsResponseProto, error) {
	res := new(hdfs.GetFsStatsResponseProto)
	fsinfo, err := opfs.StatFs()
	if err != nil {
		return res, err
	}

	res.Capacity = proto.Uint64(fsinfo.Capacity)
	res.Used = proto.Uint64(fsinfo.Used)
	res.Remaining = proto.Uint64(fsinfo.Remaining)
	res.UnderReplicated = proto.Uint64(fsinfo.UnderReplicated)
	res.CorruptBlocks = proto.Uint64(fsinfo.CorruptBlocks)
	res.MissingBlocks = proto.Uint64(fsinfo.MissingBlocks)

	return res, nil
}
