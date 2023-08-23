package cmd

import (
	"context"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func upgradeStatusDec(b []byte) (proto.Message, error) {
	req := new(hdfs.UpgradeStatusRequestProto)
	return parseRequest(b, req)
}

func upgradeStatus(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.UpgradeStatusRequestProto)
	return opfsUpgradeStatus(req)
}

func opfsUpgradeStatus(r *hdfs.UpgradeStatusRequestProto) (*hdfs.UpgradeStatusResponseProto, error) {
	return &hdfs.UpgradeStatusResponseProto {
		UpgradeFinalized: proto.Bool(true),
	}, nil
}
