package cmd

import (
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func disallowSnapshotDec(b []byte) (proto.Message, error) {
	req := new(hdfs.DisallowSnapshotRequestProto)
	return parseRequest(b, req)
}

func disallowSnapshot(m proto.Message) (proto.Message, error) {
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

	if err := globalFs.SetAllowSnapshot(false); err != nil {
		return nil, err
	}

	return new(hdfs.DisallowSnapshotResponseProto), nil
}
