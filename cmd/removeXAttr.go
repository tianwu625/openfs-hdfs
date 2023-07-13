package cmd

import (
	"log"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func removeXAttrDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RemoveXAttrRequestProto)
	return parseRequest(b, req)
}

func removeXAttr(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RemoveXAttrRequestProto)
	log.Printf("src %v\nxattr %v\n", req.GetSrc(), req.GetXAttr())
	return opfsRemoveXAttr(req)
}

func opfsRemoveXAttr(r *hdfs.RemoveXAttrRequestProto) (*hdfs.RemoveXAttrResponseProto, error) {
	src := r.GetSrc()
	xattr := r.GetXAttr()

	name := xattr.GetNamespace().String() + "." + xattr.GetName()

	log.Printf("remove key name %v", name)

	if err := opfs.RemoveXAttr(src, name); err != nil {
		return nil, err
	}

	return new(hdfs.RemoveXAttrResponseProto), nil
}
