package cmd

import (
	"log"
	"path"
	"context"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/fsmeta"
)


func deleteSnapshotDec(b []byte) (proto.Message, error) {
	req := new(hdfs.DeleteSnapshotRequestProto)
	return parseRequest(b, req)
}

func deleteSnapshot(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.DeleteSnapshotRequestProto)
	res, err := opfsDeleteSnapshot(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsDeleteSnapshotWithOpenfs(src string) error {
	f, err := opfs.Open(src)
	if err != nil {
		return err
	}

	fi, err := f.Stat()
	if err != nil {
		return err
	}
	id := fi.Size()
	log.Printf("sid %v", id)
	if err := deleteSnapWithCmd(int(id)); err != nil {
		return err
	}
	if err := opfs.RemoveFile(src); err != nil {
		return err
	}

	return nil
}

func opfsDeleteSnapshotTransaction(root, name string) error {
	srcSnapPath := path.Join(root, snapshotDir, name)
	if root == rootDir {
		srcSnapPath = path.Join(root, hdfsRootSnapDir, name)
	}

	if err := opfsDeleteSnapshotWithOpenfs(srcSnapPath); err != nil {
		return err
	}

	return nil
}


func opfsDeleteSnapshot(r *hdfs.DeleteSnapshotRequestProto) (*hdfs.DeleteSnapshotResponseProto, error) {
	root := r.GetSnapshotRoot()
	name := r.GetSnapshotName()

	if root != "/" {
		return nil, errOnlySupportRoot
	}
	gfs := fsmeta.GetGlobalFsMeta()

	if !gfs.GetAllowSnapshot() {
		return nil, errDisallowSnapshot
	}

	err := opfsDeleteSnapshotTransaction(root, name)
	if err != nil {
		return nil, err
	}

	return new(hdfs.DeleteSnapshotResponseProto), nil
}
