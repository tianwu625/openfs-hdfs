package cmd

import (
	"context"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/fsmeta"
)

func disallowSnapshotDec(b []byte) (proto.Message, error) {
	req := new(hdfs.DisallowSnapshotRequestProto)
	return parseRequest(b, req)
}

func disallowSnapshot(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.DisallowSnapshotRequestProto)
	res, err := opfsDisallowSnapshot(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsDisallowSnapshot(r *hdfs.DisallowSnapshotRequestProto)(*hdfs.DisallowSnapshotResponseProto, error) {
	root := r.GetSnapshotRoot()

	if root != "/" {
		return nil, errOnlySupportRoot
	}

	gfs := fsmeta.GetGlobalFsMeta()

	if err := gfs.SetAllowSnapshot(false); err != nil {
		return nil, err
	}

	return new(hdfs.DisallowSnapshotResponseProto), nil
}
