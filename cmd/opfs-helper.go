package cmd

import (
	"time"
	"io"
	"log"
	"path"
	"os"
	"bytes"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/openfs/openfs-hdfs/internal/opfs"
)


func opfsGetModifyTime(src string) (time.Time, error) {
	f, err := opfs.Open(src)
	if err != nil {
		return time.Time{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return time.Time{}, err
	}

	return fi.ModTime(), nil
}

const (
	hdfsSysDir = "/.hdfs.sys"
	hdfsMetaDir = hdfsSysDir + "/meta"
	hdfsMetaFile = "fileMeta"
	hdfsSysDirName = ".hdfs.sys"
)

func opfsReadAll(src string) ([]byte, error) {
	f, err := opfs.Open(src)
	if err != nil {
		log.Printf("src %v err %v", src, err)
		return []byte{}, err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return []byte{}, err
	}

	return b, nil
}

const (
	hdfsAclUser = "USER"
	hdfsAclGroup = "GROUP"
	hdfsAclMask = "MASK"
	hdfsAclOther = "OTHER"
	hdfsScopeAccess = "ACCESS"
	hdfsScopeDefault = "DEFAULT"
	hdfsScopeAll = "ALL"
)

func opfsGetPermBits(bits int, isDir bool) (uint32, error) {
	aclproto, err := opfsGetPerm(bits, isDir)
	if err != nil {
		return 0, err
	}

	return uint32(aclproto.Number()), nil
}

func opfsAclToHdfsAcl(oacls []opfs.AclGrant, isDir bool) ([]opfsHdfsAclEntry, []opfsHdfsAclEntry, error) {
	acls := make([]opfsHdfsAclEntry, 0, len(oacls))
	dacls := make([]opfsHdfsAclEntry, 0, len(oacls))
	for _, oa := range oacls {
		var a *opfsHdfsAclEntry
		switch oa.Acltype {
		case opfs.UserType:
			user := opfsGetUsername(oa.Uid)
			a = &opfsHdfsAclEntry {
				AclType: hdfsAclUser,
				AclName: user,
			}
		case opfs.GroupType:
			group := opfsGetGroupname(oa.Gid)
			a = &opfsHdfsAclEntry {
				AclType: hdfsAclGroup,
				AclName: group,
			}
		case opfs.EveryType:
			a = &opfsHdfsAclEntry {
				AclType: hdfsAclOther,
			}
		default:
			continue
		}
		perm, err := opfsGetPermBits(opfsRoundBits(oa.Aclbits, isDir), isDir)
		if err != nil {
			//log.Printf("can't convert to hdfs acl entry, skip this entry and continue")
			continue
		}
		a.AclPerm = perm
		if oa.Aclflag == opfs.AclFlagDefault{
			a.AclScope = hdfsScopeAccess
			acls = append(acls, *a)
		} else if oa.Aclflag & opfs.AclFlagInherit == opfs.AclFlagInherit {
			a.AclScope = hdfsScopeDefault
			dacls = append(dacls, *a)
		} else {
			log.Printf("can't convert this flag %v, skip this entry and continue", oa.Aclflag)
			continue
		}
	}

	return acls, dacls, nil
}

func aclsMergeBits(as []opfsHdfsAclEntry) uint32 {
	perm := uint32(0)
	for _, a := range as {
		perm |= a.AclPerm
	}

	return perm
}

func opfsGetMask(src string) (uint32,[]opfsHdfsAclEntry, uint32, []opfsHdfsAclEntry, error) {
	fi, err := getOpfsFileInfo(src)
	if err != nil {
		return 0,[]opfsHdfsAclEntry{}, 0, []opfsHdfsAclEntry{}, err
	}
	opfsAcls, err := opfs.GetAcl(src)
	if err != nil {
		log.Printf("get acl fail from opfs %v", err)
		return 0,[]opfsHdfsAclEntry{}, 0, []opfsHdfsAclEntry{}, err
	}
	//log.Printf("opfs Acls %v", opfsAcls)
	acls, dacls, err := opfsAclToHdfsAcl(opfsAcls, fi.IsDir())
	if err != nil {
		log.Printf("convert to hdfsacl fail %v", err)
		return 0,[]opfsHdfsAclEntry{}, 0, []opfsHdfsAclEntry{}, err
	}
	mask := aclsMergeBits(acls)
	dmask := aclsMergeBits(dacls)

	return mask, acls, dmask, dacls, nil
}

func opfsGetMetaPath(src string) string {
	return path.Join(hdfsMetaDir, src, hdfsMetaFile)
}

const (
	defaultSetQuota = false
	defaultNsQuota = setClrValue
)

func opfsGetFromOpfsFile(src string) (*opfsHdfsMeta, time.Time, error) {
	mask, acls, dmask, dacls, err := opfsGetMask(src)
	if err != nil {
		return nil, time.Unix(0, 0), err
	}
	log.Printf("mask %v acls %v dmask %v dacls %v", mask, acls, dmask, dacls)
	meta := &opfsHdfsMeta {
		Acl:&opfsHdfsAcl {
			SetMask: false,
			Mask: mask,
			Entries: acls,
			SetDefaultMask: false,
			DefaultMask: dmask,
			DefaultEntries: dacls,
		},
		//set default value for namespace quota
		Quota: &opfsHdfsNamespaceQuota {
			SetQuota: defaultSetQuota,
			Quota: defaultNsQuota,
		},
	}
	if err := opfsStoreConfig(src, meta); err != nil {
		return nil, time.Unix(0, 0), err
	}
	srcMeta := opfsGetMetaPath(src)
	t, err := opfsGetModifyTime(srcMeta)
	if err != nil {
		return nil, time.Unix(0, 0), err
	}
	return meta, t, nil
}

func sameopfsAclEntry (a1 *opfsHdfsAclEntry, a2 *opfsHdfsAclEntry) bool {
	if a1.AclType != a2.AclType {
		return false
	}

	if a1.AclScope != a2.AclScope {
		return false
	}

	if a1.AclName != a2.AclName {
		return false
	}

	return true
}

func updateDifferentAclEntry(nas []opfsHdfsAclEntry, oas []opfsHdfsAclEntry, mask uint32) ([]opfsHdfsAclEntry, error) {
outer:
	for _, na := range nas {
		for _, oa := range oas {
			if sameopfsAclEntry(&na, &oa) && na.AclType != hdfsAclOther {
				if mask & oa.AclPerm != na.AclPerm {
					//treat as new entry without filter mask
					oas = append(oas, na)
				}
				continue outer
			}
		}
	}
	return oas, nil
}

func updateAclMask(src string, acl *opfsHdfsAcl, updateAcl bool) error {
	mask, acls, dmask, dacls, err := opfsGetMask(src)
	if err != nil {
		return err
	}

	if !acl.SetMask {
		acl.Mask = mask
		acl.Entries = acls
	}

	if !acl.SetDefaultMask {
		acl.DefaultMask = dmask
		acl.DefaultEntries = dacls
	}
	//update acl for mask acl
	if updateAcl && acl.SetMask {
		acl.Entries, err = updateDifferentAclEntry(acls, acl.Entries, acl.Mask)
		if err != nil {
			return err
		}
	}
	if updateAcl && acl.SetDefaultMask {
		acl.DefaultEntries, err = updateDifferentAclEntry(dacls, acl.DefaultEntries, acl.DefaultMask)
		if err != nil {
			return err
		}
	}

	return nil
}

func opfsGetFromConfig(src string) (*opfsHdfsMeta, time.Time, error) {
	srcMeta := opfsGetMetaPath(src)
	b, err := opfsReadAll(srcMeta)
	if err != nil {
		if os.IsNotExist(err) {
			return opfsGetFromOpfsFile(src)
		}
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	meta := new(opfsHdfsMeta)
	if err = json.Unmarshal(b, meta); err != nil {
		return nil, time.Unix(0, 0), err
        }
	tconfig, err := opfsGetModifyTime(srcMeta)
	if err != nil {
		return nil, time.Unix(0, 0), err
	}
	tfile, err := opfsGetModifyTime(src)
	if err != nil {
		return nil, time.Unix(0, 0), err
	}
	updateAcl := tfile.After(tconfig)
	if !meta.Acl.SetMask || !meta.Acl.SetDefaultMask || updateAcl {
		if err := updateAclMask(src, meta.Acl, updateAcl); err != nil {
			return nil, time.Unix(0,0), err
		}
	}
	return meta, tconfig, nil
}

func opfsGetMetaEntry(src string) (*opfsMetaCacheEntry, error) {
	meta, tconfig, err := opfsGetFromConfig(src)
	if err != nil {
		return nil, err
	}
	e := &opfsMetaCacheEntry {
		meta: meta,
		ConfigTime: tconfig,
		StayTime: time.Now(),
	}
	e.UpdateTime, err = opfsGetModifyTime(src)
	if err != nil {
		return nil, err
	}

	return e, nil
}

func opfsDeleteMetaEntry(src string) error {
	srcMeta := opfsGetMetaPath(src)
	return opfs.RemovePath(srcMeta)
}

const (
	tmpdir = "tmp"
)

func GetRandomFileName() string {
        u, err := uuid.NewRandom()
        if err != nil {
		log.Printf("get uuid fail for tmp file name %v", err)
        }

        return u.String()
}

const (
	defaultConfigPerm = 0777
)

func cleanTmp(src string) {
	opfs.RemoveFile(src)
}

func opfsStoreConfig(src string, e *opfsHdfsMeta) error {
	srcTmp := path.Join(hdfsSysDir, tmpdir, GetRandomFileName())

	err := opfs.MakeDirAll(path.Dir(srcTmp), os.FileMode(defaultConfigPerm))
	if err != nil {
		return err
	}
	f, err := opfs.OpenWithCreate(srcTmp, os.O_WRONLY | os.O_CREATE, os.FileMode(defaultConfigPerm))
	if err != nil {
		return err
	}
	defer f.Close()
	defer cleanTmp(srcTmp)
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	data, err := json.Marshal(e)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, bytes.NewReader(data)); err != nil {
		return err
	}
	srcMeta := opfsGetMetaPath(src)
	if err := opfs.MakeDirAll(path.Dir(srcMeta), os.FileMode(defaultConfigPerm)); err != nil {
		log.Printf("mkdir fail %v", err)
		return err
	}
	if err := opfs.Rename(srcTmp, srcMeta); err != nil {
		log.Printf("rename fail %v", err)
		return err
	}

	return nil
}

func opfsIsDir(src string) (bool, error){
	f, err := opfs.Open(src)
	if err != nil {
		return false, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return false, err
	}

	return fi.IsDir(), nil
}

func opfsRenameMetaPath(src, dst string) error {
	srcMeta := opfsGetMetaPath(src)
	dstMeta := opfsGetMetaPath(dst)

	if err := opfs.MakeDirAll(path.Dir(dstMeta), defaultConfigPerm); err != nil {
		return err
	}

	if err := opfs.Rename(srcMeta, dstMeta); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	if err := opfs.RemovePath(srcMeta); err != nil {
		log.Printf("remove dir fail %v, %v", path.Dir(srcMeta), err)
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	return nil
}
