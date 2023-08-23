package cmd

import (
	"log"
	"fmt"
	"os"
	"context"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func setAclDec(b []byte) (proto.Message, error) {
	req := new(hdfs.SetAclRequestProto)
	return parseRequest(b, req)
}

func setAcl(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.SetAclRequestProto)
	log.Printf("src %v\nacls %v\n", req.GetSrc(), req.GetAclSpec())
	return opfsSetAcl(req)
}

func replaceEntriesFromProto(acls []*hdfs.AclEntryProto) *opfsHdfsAcl {
	acl := new(opfsHdfsAcl)
	for _, a := range acls {
		//process mask and default mask
		if a.GetName() == "" && a.GetType().String() == hdfs.AclEntryProto_MASK.String() {
			scope := a.GetScope().String()
			perm := a.GetPermissions().String()
			switch scope {
			case hdfs.AclEntryProto_ACCESS.String():
				acl.Mask = modePermMap[perm]
				acl.SetMask = true
			case hdfs.AclEntryProto_DEFAULT.String():
				acl.Mask = modePermMap[perm]
				acl.SetDefaultMask = true
			default:
				panic(fmt.Errorf("scope not support %v, perm %v", scope, perm))
			}
			continue
		}
		//process default user:: group:: other::
		if a.GetName() == "" && a.GetScope().String() == hdfs.AclEntryProto_DEFAULT.String() {
			atype := a.GetType().String()
			perm := a.GetPermissions().String()
			switch atype {
			case hdfs.AclEntryProto_USER.String():
				acl.SetDefaultUser = true
				acl.DefaultUser = modePermMap[perm]
			case hdfs.AclEntryProto_GROUP.String():
				acl.SetDefaultGroup = true
				acl.DefaultGroup = modePermMap[perm]
			case hdfs.AclEntryProto_OTHER.String():
				acl.SetDefaultOther = true
				acl.DefaultOther = modePermMap[perm]
			default:
				panic(fmt.Errorf("scope not support %v, perm %v", atype, perm))
			}
			continue
		}
		//process user:<name>: group:<name>: scope: default && access
		if a.GetName() != "" {
			modifyEntries(acl, a)
			continue
		}
		//only left user:: group:: other:: scope: access to invoke chmod
	}

	return acl
}
// returns Access, Default, All
func checkReplaceScope(acls []*hdfs.AclEntryProto) string {
	res := make(map[string]int)
	for _, a := range acls {
		scope := a.GetScope().String()
		if _, ok := res[scope]; !ok {
			res[scope] = 1
		}
	}

	if len(res) > 2 {
		panic(fmt.Errorf("scope should be 2 is max %v", res))
	}
	if len(res) == 2 {
		return hdfsScopeAll
	}
	if len(res) <= 0 {
		panic(fmt.Errorf("scope should be 1 is min %v", res))
	}
	for k, _ := range res {
		return k
	}

	panic(fmt.Errorf("impossible res %v", res))
}

func replaceScope(acl *opfsHdfsAcl, nacl *opfsHdfsAcl, rtype string) {
	if rtype == hdfsScopeAccess {
		acl.SetMask = nacl.SetMask
		acl.Mask = nacl.Mask
		acl.Entries = nacl.Entries
		return
	}
	if rtype == hdfsScopeDefault {
		acl.SetDefaultMask = nacl.SetDefaultMask
		acl.DefaultMask = nacl.DefaultMask
		acl.DefaultEntries = nacl.DefaultEntries
		acl.SetDefaultUser = nacl.SetDefaultUser
		acl.DefaultUser = nacl.DefaultUser
		acl.SetDefaultGroup = nacl.SetDefaultGroup
		acl.DefaultGroup = nacl.DefaultGroup
		acl.SetDefaultOther = nacl.SetDefaultOther
		acl.DefaultOther = nacl.DefaultOther
		return
	}
}

func opfsSetAcl(r *hdfs.SetAclRequestProto) (*hdfs.SetAclResponseProto, error) {
	src := r.GetSrc()
	acls := r.GetAclSpec()
	acl := new(opfsHdfsAcl)
	var err error

	replaceType := checkReplaceScope(acls)
	if replaceType == hdfsScopeAll {
		acl = replaceEntriesFromProto(acls)
	} else {
		nacl := replaceEntriesFromProto(acls)
		*acl, err = globalMeta.GetAcl(src)
		if err != nil {
			return nil, err
		}
		replaceScope(acl, nacl, replaceType)
	}

	fi, err := getOpfsFileInfo(src)
	if err != nil {
		return nil, err
	}

	opfsAcls, err := aclToOpfsAcl(acl, fi.IsDir())
	if err != nil {
		return nil, err
	}
	err = opfs.SetAcl(src, opfsAcls)
	if err != nil {
		log.Printf("setacl fail %v", err)
		return nil, err
	}
	groupperm := uint32(0)
	if acl.SetMask {
		if acl.SetGroup {
			groupperm = acl.GroupPerm
		} else {
			groupperm = (uint32(fi.Mode().Perm()) & (uint32(bitset) << groupoffset)) >> groupoffset
		}
	}

	perm, setPerm := opfsGetRWXFromAcl(fi, acl, acls)
	if setPerm {
		if acl.SetMask {
			groupperm = (uint32(perm) & (uint32(bitset) << groupoffset)) >> groupoffset
			perm &= os.FileMode((uint32(bitset) << useroffset) + acl.Mask << groupoffset +
					(uint32(bitset) << otheroffset))
		}
		if err = opfs.SetPermission(src, perm) ; err != nil {
			return nil, err
		}
		if !acl.SetMask {
			acl.Mask |= (bitset << groupoffset) & uint32(perm)
		}
	}

	if acl.SetMask {
		acl.GroupPerm = groupperm
		acl.SetGroup = true
	}

	err = globalMeta.SetAcl(src, *acl)
	if err != nil {
		log.Printf("fail to update cache %v", err)
		return nil, err
	}

	return new(hdfs.SetAclResponseProto), nil
}
