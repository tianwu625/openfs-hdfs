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

func modifyAclEntriesDec(b []byte) (proto.Message, error) {
	req := new(hdfs.ModifyAclEntriesRequestProto)
	return parseRequest(b, req)
}

func modifyAclEntries(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.ModifyAclEntriesRequestProto)
	log.Printf("src %v\naclspec %v\n", req.GetSrc(), req.GetAclSpec())
	return opfsSetAclEntries(req)
}

func opfsGetAclBits(perm uint32, isDir bool) (int) {
	log.Printf("perm %v", perm)
	bits := 0
	switch perm {
	case 0:
		bits = 0
	case 1:
		bits = opfs.AclExcute
	case 2:
		if isDir {
			bits = opfs.AclDirWrite
		} else {
			bits = opfs.AclFileWrite
		}
	case 3:
		if isDir {
			bits = opfs.AclDirWrite | opfs.AclExcute
		} else {
			bits = opfs.AclFileWrite | opfs.AclExcute
		}
	case 4:
		if isDir {
			bits = opfs.AclDirRead
		} else {
			bits = opfs.AclFileRead
		}
	case 5:
		if isDir {
			bits = opfs.AclDirRead
		} else {
			bits = opfs.AclFileRead
		}
	case 6:
		if isDir {
			bits = opfs.AclDirRead | opfs.AclDirWrite
		} else {
			bits = opfs.AclFileRead | opfs.AclFileWrite
		}
	case 7:
		if isDir {
			bits = opfs.AclDirFullControl
		} else {
			bits = opfs.AclFileFullControl
		}
	default:
		panic(fmt.Errorf("perm value is not support %v", perm))
	}

	return bits
}

func HdfsAclToOpfsAcl(acls []opfsHdfsAclEntry, mask uint32, isDir bool) []opfs.AclGrant {
	res := make([]opfs.AclGrant, 0, len(acls))

	for _, a := range acls {
		oacl := opfs.AclGrant{}
		switch a.AclType {
		case hdfsAclUser:
			user := a.AclName
			bits := opfsGetAclBits(a.AclPerm & mask, isDir)
			uid, err := opfsGetUid(user)
			if err != nil {
				return res
			}
			oacl = opfs.AclGrant {
				Uid: uid,
				Acltype: opfs.UserType,
				Aclbits: bits,
			}
		case hdfsAclGroup:
			group := a.AclName
			bits := opfsGetAclBits(a.AclPerm & mask, isDir)
			gid, err := opfsGetGid(group)
			if err != nil {
				return res
			}
			oacl = opfs.AclGrant {
				Gid: gid,
				Acltype: opfs.GroupType,
				Aclbits: bits,
			}
		case hdfsAclOther:
			//everyone
			bits := opfsGetAclBits(a.AclPerm & mask, isDir)
			oacl = opfs.AclGrant {
				Acltype: opfs.EveryType,
				Aclbits: bits,
			}
		default:
			panic(fmt.Errorf("perm value is not support %v", a.AclPerm))
		}
		oacl.Aclflag = opfs.AclFlagDefault
		switch a.AclScope {
		case hdfsScopeAccess:
		case hdfsScopeDefault:
			oacl.Aclflag = opfs.AclFlagOnlyInherit
		default:
			panic(fmt.Errorf("scope value is not support %v", a.AclScope))
		}
		res = append(res, oacl)
	}

	return res
}


func aclToOpfsAcl(acl *opfsHdfsAcl, isDir bool) ([]opfs.AclGrant, error) {
	mask := uint32(bitset)
	if acl.SetMask {
		mask = acl.Mask
	}
	res := HdfsAclToOpfsAcl(acl.Entries, mask, isDir)
	dmask := uint32(bitset)
	if acl.SetDefaultMask {
		dmask = acl.DefaultMask
	}
	dres := HdfsAclToOpfsAcl(acl.DefaultEntries, dmask, isDir)

	res = append(res, dres...)

	return res, nil
}

func sameElem(a1, a2 opfs.AclGrant) bool {
	if a1.Acltype != a2.Acltype {
		return false
	}
	switch a1.Acltype {
	case opfs.UserType:
		return a1.Uid == a2.Uid
	case opfs.GroupType:
		return a1.Gid == a2.Gid
	case opfs.EveryType:
		return true
	default:
		return false
	}

	return false
}

func opfsMergeAcls(olds, news []opfs.AclGrant) []opfs.AclGrant {
	for _, n := range news {
		for i, o := range olds {
			if sameElem(n, o) {
				olds = append(olds[:i], olds[i+1:]...)
				break
			}
		}
		olds = append(olds, n)
	}
	return olds
}

const (
	useroffset = 6
	groupoffset = 3
	otheroffset = 0
	bitset = 7
)

var (
	modePermMap map[string]uint32 = map[string]uint32{
		"NONE":0,
		"EXECUTE":1,
		"WRITE":2,
		"WRITE_EXECUTE":3,
		"READ":4,
		"READ_EXECUTE":5,
		"READ_WRITE":6,
		"PERM_ALL":7,
	}
)

func opfsModeToPerm(modename string) (os.FileMode) {
	perm, ok := modePermMap[modename]
	if !ok {
		panic(fmt.Errorf("not support modename %v", modename))
	}

	return os.FileMode(perm)
}


func opfsComposePerm(fi os.FileInfo, acl *opfsHdfsAcl) uint32 {
	perm := uint32(fi.Mode().Perm())
	userperm := perm & (uint32(bitset) << useroffset)
	groupperm := acl.Mask << groupoffset
	otherperm := perm & (uint32(bitset) << otheroffset)

	perm = userperm + groupperm + otherperm

	return perm
}


func opfsReplaceMode(perm os.FileMode, modetype, modename string) (os.FileMode) {
	userperm := perm & (bitset << useroffset)
	groupperm := perm & (bitset << groupoffset)
	otherperm := perm & (bitset << otheroffset)
	switch modetype {
	case hdfsAclUser:
		userperm = opfsModeToPerm(modename) << useroffset
	case hdfsAclGroup:
		groupperm = opfsModeToPerm(modename) << groupoffset
	case hdfsAclOther:
		otherperm = opfsModeToPerm(modename) << otheroffset
	default:
		panic(fmt.Errorf("not support %v", modetype))
	}
	perm = userperm + groupperm + otherperm

	return perm
}

func opfsGetRWXFromAcl(fi os.FileInfo, acl *opfsHdfsAcl, acls []*hdfs.AclEntryProto) (perm os.FileMode, setPerm bool) {
	perm = fi.Mode().Perm()
	if acl.SetGroup {
		userperm := perm & (bitset << useroffset)
		otherperm := perm & (bitset << otheroffset)
		perm = userperm + (os.FileMode(acl.GroupPerm) << groupoffset) + otherperm
	}
	for _, a := range acls {
		if a.GetName() == "" && a.GetScope().String() != hdfsScopeDefault {
			if a.GetType().String() != hdfsAclMask {
				perm = opfsReplaceMode(perm, a.GetType().String(), a.GetPermissions().String())
			}
			if !setPerm {
				setPerm = true
			}
		}
	}
	return perm, setPerm
}

func replaceAndAppendEntries(acls []opfsHdfsAclEntry, a *opfsHdfsAclEntry) []opfsHdfsAclEntry {
	found := false
	for i, acl := range acls {
		//replace
		if sameopfsAclEntry(&acl, a) {
			found = true
			acls[i] = *a
			continue
		}
	}
	//append
	if !found {
		acls = append(acls, *a)
	}
	return acls
}

func modifyEntries(acl *opfsHdfsAcl, a *hdfs.AclEntryProto) {
	opfsacl := &opfsHdfsAclEntry {
		AclType: a.GetType().String(),
		AclScope: a.GetScope().String(),
		AclPerm: modePermMap[a.GetPermissions().String()],
		AclName: a.GetName(),
	}
	switch opfsacl.AclScope {
	case hdfs.AclEntryProto_ACCESS.String():
		acl.Entries = replaceAndAppendEntries(acl.Entries, opfsacl)
	case hdfs.AclEntryProto_DEFAULT.String():
		acl.DefaultEntries = replaceAndAppendEntries(acl.DefaultEntries, opfsacl)
	default:
		panic(fmt.Errorf("get new entry %v", opfsacl))
	}
}

//replace and append acl entry
func modifyEntriesFromProto(acl *opfsHdfsAcl, acls []*hdfs.AclEntryProto) {
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
}


func opfsSetAclEntries(r *hdfs.ModifyAclEntriesRequestProto)(*hdfs.ModifyAclEntriesResponseProto, error) {
	src := r.GetSrc()
	acls := r.GetAclSpec()

	acl, err := globalMeta.GetAcl(src)
	if err != nil {
		log.Printf("get acl from cache fail")
		return nil, err
	}

	modifyEntriesFromProto(&acl, acls)

	fi, err := getOpfsFileInfo(src)
	if err != nil {
		return nil, err
	}

	opfsAcls, err := aclToOpfsAcl(&acl, fi.IsDir())
	if err != nil {
		return new(hdfs.ModifyAclEntriesResponseProto), err
	}
	err = opfs.SetAcl(src, opfsAcls)
	if err != nil {
		log.Printf("setacl fail %v", err)
		return nil, err
	}
	groupperm := uint32(0)
	if acl.SetMask || len(acl.Entries) != 0 {
		if acl.SetGroup {
			groupperm = acl.GroupPerm
		} else {
			groupperm = (uint32(fi.Mode().Perm()) & (uint32(bitset) << groupoffset)) >> groupoffset
		}
	}
	perm, setPerm := opfsGetRWXFromAcl(fi, &acl, acls)
	if setPerm {
		if acl.SetMask {
			groupperm = (uint32(perm) & (uint32(bitset) << groupoffset)) >> groupoffset
			perm &= os.FileMode((uint32(bitset) << useroffset) + acl.Mask << groupoffset +
					(uint32(bitset) << otheroffset))
			log.Printf("SetPermission perm %v", perm)
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

	err = globalMeta.SetAcl(src, acl)
	if err != nil {
		log.Printf("fail to update cache %v", err)
		return nil, err
	}

	return new(hdfs.ModifyAclEntriesResponseProto), nil
}
