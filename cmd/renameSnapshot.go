package cmd

import (
	"os"
	"path"
	"log"
	"context"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)


func renameSnapshotDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RenameSnapshotRequestProto)
	return parseRequest(b, req)
}

func renameSnapshot(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RenameSnapshotRequestProto)
	res, err := opfsRenameSnapshot(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsRenameSnapshot(r *hdfs.RenameSnapshotRequestProto) (*hdfs.RenameSnapshotResponseProto, error) {
	root := r.GetSnapshotRoot()
	oldname := r.GetSnapshotOldName()
	newname := r.GetSnapshotNewName()

	srcSnapPathOld := path.Join(root, snapshotDir, oldname)
	if root == rootDir {
		srcSnapPathOld = path.Join(root, hdfsRootSnapDir, oldname)
	}
	f, err := opfs.Open(srcSnapPathOld)
	if err != nil {
		log.Printf("old fail %v, %v", srcSnapPathOld,err)
		return nil, err
	}
	f.Close()
	srcSnapPathNew := path.Join(root, snapshotDir, newname)
	if root == rootDir {
		srcSnapPathNew = path.Join(root, hdfsRootSnapDir, newname)
	}
	f, err = opfs.Open(srcSnapPathNew)
	if err == nil ||
	(err != nil && !os.IsNotExist(err)) {
		log.Printf("new fail %v, %v", srcSnapPathNew, err)
		if err == nil {
			return nil, os.ErrExist
		}
		return nil, err
	}
	err = opfs.Rename(srcSnapPathOld, srcSnapPathNew)
	if err != nil {
		return nil, err
	}

	return new(hdfs.RenameSnapshotResponseProto), nil
}
