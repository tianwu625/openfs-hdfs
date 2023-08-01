package cmd

import (
	"log"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func removeAclDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RemoveAclRequestProto)
	return parseRequest(b, req)
}

func removeAcl(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RemoveAclRequestProto)
	log.Printf("src %v\n", req.GetSrc())
	return opfsRemoveAcl(req)
}

func opfsRemoveAcl(r *hdfs.RemoveAclRequestProto) (*hdfs.RemoveAclResponseProto, error) {
	src := r.GetSrc()
	opfsAcls := make([]opfs.AclGrant, 0)
	err := opfs.SetAcl(src, opfsAcls)
	if err != nil {
		log.Printf("fail to setAcl %v", err)
		return nil, err
	}
	acl := opfsHdfsAcl{}
	if err := globalMeta.SetAcl(src, acl); err != nil {
		return nil, err
	}

	return new(hdfs.RemoveAclResponseProto), nil
}
