package cmd

import (
	"log"
	"path"
	"os"
	iofs "io/fs"
	"fmt"
	"os/exec"
	"strings"
	"context"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getAclStatusDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetAclStatusRequestProto)
	return parseRequest(b, req)
}

func getAclStatus(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetAclStatusRequestProto)
	log.Printf("src %v\n", req.GetSrc())
	return opfsGetAclStatus(req)
}

func getOpfsFileInfo(src string) (fi os.FileInfo, err error) {
	f, err := opfs.Open(src)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err = f.Stat()
	if err != nil {
		return nil, err
	}
	return fi, nil
}

func opfsGetPerm(bits int, isDir bool) (hdfs.AclEntryProto_FsActionProto, error) {
	res := hdfs.AclEntryProto_NONE
	if isDir {
		switch bits {
		case 0:
		case opfs.AclExcute:
			res = hdfs.AclEntryProto_EXECUTE
		case opfs.AclDirWrite:
			res = hdfs.AclEntryProto_WRITE
		case (opfs.AclDirWrite | opfs.AclExcute):
			res = hdfs.AclEntryProto_WRITE_EXECUTE
		case opfs.AclDirRead:
			res = hdfs.AclEntryProto_READ
		case (opfs.AclDirRead | opfs.AclExcute):
			res = hdfs.AclEntryProto_READ_EXECUTE
		case (opfs.AclDirRead | opfs.AclDirWrite):
			res = hdfs.AclEntryProto_READ_WRITE
		case opfs.AclDirFullControl:
			res = hdfs.AclEntryProto_PERM_ALL
		default:
			return res, fmt.Errorf("bits %v %v can't convert to hdfs Acl", bits, isDir)
		}
	} else {
		switch bits {
		case 0:
		case opfs.AclExcute:
			res = hdfs.AclEntryProto_EXECUTE
		case opfs.AclFileWrite:
			res = hdfs.AclEntryProto_WRITE
		case (opfs.AclFileWrite | opfs.AclExcute):
			res = hdfs.AclEntryProto_WRITE_EXECUTE
		case opfs.AclFileRead:
			res = hdfs.AclEntryProto_READ
		case (opfs.AclFileRead | opfs.AclExcute):
			res = hdfs.AclEntryProto_READ_EXECUTE
		case (opfs.AclFileRead | opfs.AclFileWrite):
			res = hdfs.AclEntryProto_READ_WRITE
		case opfs.AclFileFullControl:
			res = hdfs.AclEntryProto_PERM_ALL
		default:
			return res, fmt.Errorf("bits %v %v can't convert to hdfs Acl", bits, isDir)
		}
	}

	return res, nil
}

const (
	OpfsUsername = "openfs-get-username"
	OpfsGroupname = "openfs-get-groupname"
)

func opfsGetUsername(uid int) string {
	cmdPath := path.Join(OpfsCmdDir, OpfsUsername)
	c := exec.Command(cmdPath, fmt.Sprintf("%d", uid))
	output, err := c.CombinedOutput()
	if err != nil {
		return fmt.Sprintf("%d", uid)
	}

	return strings.TrimSuffix(string(output), "\n")
}

func opfsGetGroupname(gid int) string {
	cmdPath := path.Join(OpfsCmdDir, OpfsGroupname)
	c := exec.Command(cmdPath, fmt.Sprintf("%d", gid))
	output, err := c.CombinedOutput()
	if err != nil {
		return fmt.Sprintf("%d", gid)
	}
	return strings.TrimSuffix(string(output), "\n")
}

func opfsRoundBits(bits int, isDir bool) int {
	return bits
}

const (
	ProtoMaxEntries = 5 //include mask, default:user, default:mask, default:group, default:other
)

var (
	permProtoMap map[uint32]hdfs.AclEntryProto_FsActionProto = map[uint32]hdfs.AclEntryProto_FsActionProto {
		0:hdfs.AclEntryProto_NONE,
		1:hdfs.AclEntryProto_EXECUTE,
		2:hdfs.AclEntryProto_WRITE,
		3:hdfs.AclEntryProto_WRITE_EXECUTE,
		4:hdfs.AclEntryProto_READ,
		5:hdfs.AclEntryProto_READ_EXECUTE,
		6:hdfs.AclEntryProto_READ_WRITE,
		7:hdfs.AclEntryProto_PERM_ALL,
	}
	typeProtoMap map[string]hdfs.AclEntryProto_AclEntryTypeProto = map[string]hdfs.AclEntryProto_AclEntryTypeProto {
		hdfsAclUser:hdfs.AclEntryProto_USER,
		hdfsAclGroup:hdfs.AclEntryProto_GROUP,
		hdfsAclMask:hdfs.AclEntryProto_MASK,
		hdfsAclOther:hdfs.AclEntryProto_OTHER,
	}
	scopeProtoMap map[string]hdfs.AclEntryProto_AclEntryScopeProto = map[string]hdfs.AclEntryProto_AclEntryScopeProto {
		hdfsScopeAccess:hdfs.AclEntryProto_ACCESS,
		hdfsScopeDefault:hdfs.AclEntryProto_DEFAULT,
	}
)

func getAclByType(entries []opfsHdfsAclEntry, getType string) []*hdfs.AclEntryProto {
	res := make([]*hdfs.AclEntryProto, 0, len(entries))
	for _, e := range entries {
		if getType != e.AclType {
			continue
		}
		a := &hdfs.AclEntryProto {
			Type: typeProtoMap[e.AclType].Enum(),
			Scope: scopeProtoMap[e.AclScope].Enum(),
			Permissions:permProtoMap[e.AclPerm].Enum(),
			Name:proto.String(e.AclName),
		}
		res = append(res, a)
	}

	return res
}

func HdfsAclToProto(acl *opfsHdfsAcl, fi os.FileInfo) []*hdfs.AclEntryProto {
	res := make([]*hdfs.AclEntryProto, 0, len(acl.Entries) + len(acl.DefaultEntries) + ProtoMaxEntries)

	//1. add user type for scope Access
	userAcls := getAclByType(acl.Entries, hdfsAclUser)
	res = append(res, userAcls...)

	//2. group bits is an entry in acl
	if acl.SetMask || len(acl.Entries) != 0 {
		groupperm := (uint32(fi.Mode().Perm()) & (uint32(bitset) << groupoffset)) >> groupoffset
		if acl.SetGroup {
			groupperm = acl.GroupPerm
		}
		a := &hdfs.AclEntryProto {
			Type: typeProtoMap[hdfsAclGroup].Enum(),
			Scope: scopeProtoMap[hdfsScopeAccess].Enum(),
			Permissions: permProtoMap[groupperm].Enum(),
			Name:proto.String(""),
		}
		res = append(res, a)
	}
	//3. add group type for scope Access
	groupAcls := getAclByType(acl.Entries, hdfsAclGroup)
	res = append(res, groupAcls...)
	//4. mask and other ignore


	//5. default user
	if len(acl.DefaultEntries) != 0 || acl.SetDefaultUser {
		mask := uint32(bitset)
		if acl.SetDefaultUser {
			mask = acl.DefaultUser
		}
		a := &hdfs.AclEntryProto {
			Type: typeProtoMap[hdfsAclUser].Enum(),
			Scope: scopeProtoMap[hdfsScopeDefault].Enum(),
			Permissions: permProtoMap[mask].Enum(),
			Name:proto.String(""),
		}
		res = append(res, a)
	}
	duserAcls := getAclByType(acl.DefaultEntries, hdfsAclUser)
	res = append(res, duserAcls...)
	//6. default group
	if len(acl.DefaultEntries) != 0 || acl.SetDefaultGroup {
		mask := uint32(bitset)
		if acl.SetDefaultGroup {
			mask = acl.DefaultGroup
		}
		a := &hdfs.AclEntryProto {
			Type: typeProtoMap[hdfsAclGroup].Enum(),
			Scope: scopeProtoMap[hdfsScopeDefault].Enum(),
			Permissions: permProtoMap[mask].Enum(),
			Name:proto.String(""),
		}
		res = append(res, a)
	}
	dgroupAcls := getAclByType(acl.DefaultEntries, hdfsAclGroup)
	res = append(res, dgroupAcls...)
	//7. default mask
	if len(acl.DefaultEntries) != 0 || acl.SetDefaultMask {
		mask := uint32(bitset)
		if acl.SetDefaultMask {
			mask = acl.DefaultMask
		}
		a := &hdfs.AclEntryProto{
			Type: typeProtoMap[hdfsAclMask].Enum(),
			Scope: scopeProtoMap[hdfsScopeDefault].Enum(),
			Permissions: permProtoMap[mask].Enum(),
			Name:proto.String(""),
		}
		res = append(res, a)
	}
	//8. default other
	if len(acl.DefaultEntries) != 0 || acl.SetDefaultOther {
		mask := uint32(bitset)
		if acl.SetDefaultOther {
			mask = acl.DefaultOther
		}
		a := &hdfs.AclEntryProto {
			Type: typeProtoMap[hdfsAclOther].Enum(),
			Scope: scopeProtoMap[hdfsScopeDefault].Enum(),
			Permissions: permProtoMap[mask].Enum(),
			Name:proto.String(""),
		}
		res = append(res, a)
	}

	return res
}

func opfsGetPermWithAcl(src string) (uint32, bool, error) {
	fi, _ := getOpfsFileInfo(src)
	allPerm := uint32(fi.Mode().Perm())
	setAcl := false
	gmetas := getGlobalMeta()
	acl, err := gmetas.GetAcl(src)
	if err != nil {
		return allPerm, setAcl, err
	}
	if acl.SetMask || len(acl.Entries) != 0 {
		allPerm = opfsComposePerm(fi, &acl)
	}

	return allPerm, setAcl, nil
}

func opfsGetAclStatus(r *hdfs.GetAclStatusRequestProto) (*hdfs.GetAclStatusResponseProto, error) {
	src := r.GetSrc()
	gmetas := getGlobalMeta()
	acl, err := gmetas.GetAcl(src)
	if err != nil {
		log.Printf("get acl from cache fail")
		return nil,err
	}

	log.Printf("get from cache acl %v", acl)

	fi, err := getOpfsFileInfo(src)
	if err != nil {
		return nil, err
	}

	acls := HdfsAclToProto(&acl, fi)
	if err != nil {
		return nil, err
	}
	log.Printf("proto acls %v", acls)

	opfsStat, err := opfsgetSysInfo(fi)
	if err != nil {
		return nil, err
	}

	perm := uint32(fi.Mode().Perm())
	if acl.SetMask || len(acl.Entries) != 0 {
		perm = opfsComposePerm(fi, &acl)
	}
	log.Printf("perm %o", perm)
	res := &hdfs.AclStatusProto {
		Owner: proto.String(opfsGetUsername(int(opfsStat.Uid))),
		Group: proto.String(opfsGetGroupname(int(opfsStat.Gid))),
		Sticky: proto.Bool(fi.Mode() & iofs.ModeSticky != 0),
		Entries: acls,
		Permission: &hdfs.FsPermissionProto {
			Perm: proto.Uint32(perm),
		},
	}

	return &hdfs.GetAclStatusResponseProto {
		Result: res,
	}, nil
}
