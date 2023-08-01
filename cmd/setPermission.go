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
	log.Printf("src %v\nperm %o\n", req.GetSrc(), req.GetPermission().GetPerm())
	return opfsSetPermission(req)
}

func opfsSetPermission(r *hdfs.SetPermissionRequestProto) (*hdfs.SetPermissionResponseProto, error) {
	res := new(hdfs.SetPermissionResponseProto)
	src := r.GetSrc()
	perm := r.GetPermission().GetPerm()

	acl, err := globalMeta.GetAcl(src)
	if !acl.SetMask && len(acl.Entries) == 0 {
		if err := opfs.SetPermission(src, os.FileMode(perm)); err != nil {
			return nil, err
		}

		return res, nil
	}
	fi, err := getOpfsFileInfo(src)
	if err != nil {
		return nil, err
	}
	acl.SetMask = true
	acl.Mask = (perm & (bitset << groupoffset)) >> groupoffset
	opfsAcls, err := aclToOpfsAcl(&acl, fi.IsDir())
	if err != nil {
		return nil, err
	}
	err = opfs.SetAcl(src, opfsAcls)
	if err != nil {
		log.Printf("setacl fail %v", err)
		return nil, err
	}
	allPerm := uint32(fi.Mode().Perm())
	groupperm := uint32(0)
	if acl.SetGroup {
		groupperm = acl.GroupPerm
	} else {
		groupperm = (allPerm & (uint32(bitset) << groupoffset)) >> groupoffset
	}
	acl.SetGroup = true
	acl.GroupPerm = groupperm
	userperm := perm & (bitset << useroffset)
	otherperm := perm & (bitset << otheroffset)
	perm = userperm + ((groupperm & acl.Mask) << groupoffset) + otherperm
	if err := opfs.SetPermission(src, os.FileMode(perm)); err != nil {
		return nil, err
	}
	err = globalMeta.SetAcl(src, acl)
	if err != nil {
		log.Printf("fail to update cache %v", err)
		return nil, err
	}
	return res, nil
}
