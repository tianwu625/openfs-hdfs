package cmd

import (
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func finalizeUpgradeDec(b []byte) (proto.Message, error) {
	req := new(hdfs.FinalizeUpgradeRequestProto)
	return parseRequest(b, req)
}

func finalizeUpgrade(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.FinalizeUpgradeRequestProto)
	return opfsFinalizeUpgrade(req)
}

func opfsFinalizeUpgrade(r *hdfs.FinalizeUpgradeRequestProto) (*hdfs.FinalizeUpgradeResponseProto, error) {

	return &hdfs.FinalizeUpgradeResponseProto{}, nil
}
