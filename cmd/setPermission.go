package cmd

import (
	"log"
	"os"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func setPermissionDec(b []byte) (proto.Message, error) {
	req := new(hdfs.SetPermissionRequestProto)
	return parseRequest(b, req)
}

func setPermission(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.SetPermissionRequestProto)
	log.Printf("src %v\nperm %v\n", req.GetSrc(), req.GetPermission().GetPerm())
	return opfsSetPermission(req)
}

func opfsSetPermission(r *hdfs.SetPermissionRequestProto) (*hdfs.SetPermissionResponseProto, error) {
	res := new(hdfs.SetPermissionResponseProto)
	src := r.GetSrc()
	perm := r.GetPermission().GetPerm()

	opfs.SetPermission(src, os.FileMode(perm))
	return res, nil
}
