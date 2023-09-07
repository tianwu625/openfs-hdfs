package cmd

import (
	"context"
	"log"
	"errors"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/fsmeta"
)

func allowSnapshotDec(b []byte) (proto.Message, error) {
	req := new(hdfs.AllowSnapshotRequestProto)
	return parseRequest(b, req)
}

func allowSnapshot(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.AllowSnapshotRequestProto)
	log.Printf("snapshotRoot %v", req.GetSnapshotRoot())
	res, err := opfsAllowSnapshot(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

var errOnlySupportRoot error = errors.New("openfs hdfs only support root directory allow snapshot")

func opfsAllowSnapshot(r *hdfs.AllowSnapshotRequestProto) (*hdfs.AllowSnapshotResponseProto, error) {
	root := r.GetSnapshotRoot()
	if root != "/" {
		return nil, errOnlySupportRoot
	}
	gfs := fsmeta.GetGlobalFsMeta()

	if err := gfs.SetAllowSnapshot(true); err != nil {
		return nil, err
	}

	return new(hdfs.AllowSnapshotResponseProto), nil
}
