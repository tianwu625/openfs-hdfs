package cmd

import (
	"context"
	"log"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func removeDefaultAclDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RemoveDefaultAclRequestProto)
	return parseRequest(b, req)
}

func removeDefaultAcl(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RemoveDefaultAclRequestProto)
	log.Printf("src %v\n", req.GetSrc())
	return opfsRemoveDefaultAcl(req)
}

func cleanDefaultAcl(acl *opfsHdfsAcl) {
	acl.DefaultEntries = make([]opfsHdfsAclEntry, 0)
	acl.SetDefaultUser = false
	acl.DefaultUser = 0
	acl.SetDefaultGroup = false
	acl.DefaultGroup = 0
	acl.SetDefaultMask = false
	acl.DefaultMask = 0
	acl.SetDefaultOther = false
	acl.DefaultOther = 0
}

func opfsRemoveDefaultAcl(r *hdfs.RemoveDefaultAclRequestProto) (*hdfs.RemoveDefaultAclResponseProto, error) {
	src := r.GetSrc()

	acl, err := globalMeta.GetAcl(src)
	if err != nil {
		log.Printf("get acl from cache fail")
		return nil, err
	}

	cleanDefaultAcl(&acl)

	fi, err := getOpfsFileInfo(src)
	if err != nil {
		return nil, err
	}

	opfsAcls, err := aclToOpfsAcl(&acl, fi.IsDir())
	if err != nil {
		return nil, err
	}
	err = opfs.SetAcl(src, opfsAcls)
	if err != nil {
		log.Printf("setacl fail %v", err)
		return nil, err
	}

	err = globalMeta.SetAcl(src, acl)
	if err != nil {
		log.Printf("fail to update cache %v", err)
		return nil, err
	}

	return new(hdfs.RemoveDefaultAclResponseProto), nil
}

