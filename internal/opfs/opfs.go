package opfs

//#cgo CFLAGS: -I /usr/include
//#cgo LDFLAGS: -L /usr/lib64 -lofapi
//#include <sys/time.h>
//#include <stdlib.h>
//#include "glusterfs/api/ofapi.h"
//#include <linux/stat.h>
import "C"

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"
	"unsafe"
	"log"
	pathutils "path"
)

type opfsCroot struct {
	mutex    sync.Mutex
	init     bool
	fs       *C.ofapi_fs_t
	rootpath string
}

var root opfsCroot

func opfsErr(ret C.int) error {
	if ret < C.int(0) {
		ret = -ret
	}

	return syscall.Errno(int(ret))
}

func Init(fsPath string) (err error) {
	root.mutex.Lock()
	defer root.mutex.Unlock()
	if root.init {
		return nil
	}
	root.init = true
	root.fs = C.ofapi_init(C.CString("localhost"), C.int(1306), C.int(1), C.CString("/dev/null"), C.CString(""))
	root.rootpath = fsPath
	return nil
}

const cok = C.int(0)

func open(path string) (fd *C.ofapi_fd_t, err error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	ret := C.ofapi_open(root.fs, cpath, 0, &fd)
	if ret != cok {
		return nil, opfsErr(ret)
	}
	return fd, nil
}

func createFile(path string, perm os.FileMode) error {
	dirpath, filename := filepath.Split(path)
	pfd, err := open(dirpath)
	if err != nil {
		return err
	}
	defer C.ofapi_close(pfd)
	cfilename := C.CString(filename)
	defer C.free(unsafe.Pointer(cfilename))
	ret := C.ofapi_creatat(pfd, cfilename, C.uint32_t(perm))
	if ret != cok {
		return opfsErr(ret)
	}
	return nil
}

func OpenWithCreate(path string, flag int, perm os.FileMode) (*OpfsFile, error) {
	var opf OpfsFile
	var fd *C.ofapi_fd_t
	fd, err := open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) && (flag&os.O_CREATE != 0) {
			err := createFile(path, perm)
			if err != nil {
				return nil, err
			}
			fd, err = open(path)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	opf.fd = fd
	opf.flags = flag
	_, opf.name = filepath.Split(path)
	if flag&os.O_TRUNC != 0 {
		ret := C.ofapi_truncate(opf.fd, C.uint64_t(0))
		if ret != cok {
			return nil, opfsErr(ret)
		}
	}
	if flag&os.O_APPEND != 0 {
		var oatt C.struct_oatt
		ret := C.ofapi_getattr(opf.fd, &oatt)
		if ret != cok {
			return nil, opfsErr(ret)
		}
		opf.offset = int64(oatt.oa_size)
	}
	return &opf, nil
}

func toCUint32Array(s []int) (*C.uint32_t, C.uint32_t) {
	sHdr := (*reflect.SliceHeader)(unsafe.Pointer(&s))
	cs := (*C.uint32_t)(unsafe.Pointer(uintptr(sHdr.Data)))
	cl := (C.uint32_t)(sHdr.Len)
	return cs, cl
}

func SetCred(uid int, gids []int) error {
	C.ofapi_setcred(C.uint32_t(uid), C.uint32_t(gids[0]))
	return nil
}

const (
	SlashSeparator = "/"
	defaultPerm    = 0700
)

func MakeDir(path string, perm os.FileMode) error {
	dir, name := filepath.Split(path)
	pfd, err := open(dir)
	if err != nil {
		return err
	}
	defer C.ofapi_close(pfd)
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	ret := C.ofapi_mkdirat(pfd, cname, C.uint32_t(perm))
	if ret != cok {
		return opfsErr(ret)
	}
	return nil
}

func RemoveFile(path string) error {
	dir, name := filepath.Split(strings.TrimSuffix(path, SlashSeparator))
	pfd, err := open(dir)
	if err != nil {
		return err
	}
	defer C.ofapi_close(pfd)
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	ret := C.ofapi_unlinkat(pfd, cname)
	if ret != cok && !errors.Is(opfsErr(ret), os.ErrNotExist) {
		return opfsErr(ret)
	}
	return nil
}

func RemoveDir(path string) error {
	dir, name := filepath.Split(strings.TrimSuffix(path, SlashSeparator))
	pfd, err := open(dir)
	if err != nil {
		return err
	}
	defer C.ofapi_close(pfd)
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	ret := C.ofapi_rmdirat(pfd, cname)
	if ret != cok {
		return opfsErr(ret)
	}
	return nil
}

func Rename(src, dst string) error {
	sdir, sname := filepath.Split(src)
	ddir, dname := filepath.Split(dst)

	psfd, err := open(sdir)
	if err != nil {
		return err
	}
	defer C.ofapi_close(psfd)
	pdfd, err := open(ddir)
	if err != nil {
		return err
	}
	defer C.ofapi_close(pdfd)

	csname := C.CString(sname)
	defer C.free(unsafe.Pointer(csname))
	cdname := C.CString(dname)
	defer C.free(unsafe.Pointer(cdname))
	ret := C.ofapi_renameat(psfd, csname, pdfd, cdname)
	if ret != cok {
		return opfsErr(ret)
	}

	return nil
}

func ReadDir(path string) (name string, err error) {
	path = strings.TrimSuffix(path, SlashSeparator)
	fd, err := open(path)
	if err != nil {
		return "", err
	}
	defer C.ofapi_close(fd)

	var dent C.struct_dirent
	var oatt C.struct_oatt
	for {
		ret := C.ofapi_readdirp(fd, &dent, &oatt)
		if ret == cok {
			return "", nil
		}

		dname := C.GoString(&dent.d_name[0])

		if dname == "." || dname == ".." {
			continue
		}

		switch int(dent.d_type) {
		case 1: //file regular
		case 2:
			dname += SlashSeparator
			return dname, nil
		default:
			return dname, opfsErr(C.int(95))
		}
	}

	return "", nil
}

func Stat(path string) (os.FileInfo, error) {
	var ofi OpfsInfo

	fd, err := open(strings.TrimSuffix(path, SlashSeparator))
	if err != nil {
		return nil, err
	}
	defer C.ofapi_close(fd)

	ret := C.ofapi_getattr(fd, &ofi.stat)
	if ret != cok {
		return nil, opfsErr(ret)
	}

	ofi.name = filepath.Base(strings.TrimSuffix(path, SlashSeparator))

	return ofi, nil
}

func Open(path string) (*OpfsFile, error) {
	var opf OpfsFile

	fd, err := open(path)
	if err != nil {
		return nil, err
	}

	opf.fd = fd
	opf.flags = os.O_RDONLY
	opf.name = filepath.Base(path)

	return &opf, nil
}

func Utime(path string, mt time.Time, mset bool, at time.Time, aset bool) error {
	var uatime C.struct_timeval
	var umtime C.struct_timeval
	var uap,ump *C.struct_timeval

	if mset {
		umtime.tv_sec = C.long(mt.Unix())
		ump = &umtime
	}

	if aset {
		uatime.tv_sec = C.long(at.Unix())
		uap = &uatime
	}

	fd, err := open(strings.TrimSuffix(path, SlashSeparator))
	if err != nil {
		return err
	}
	defer C.ofapi_close(fd)
	ret := C.ofapi_utime(fd, uap, ump)
	if ret != cok {
		return opfsErr(ret)
	}

	return nil
}

const (
	AclDirRead = int(C.OFAPI_ACE_MASK_DIR_LIST | C.OFAPI_ACE_MASK_XATTR_READ |
		C.OFAPI_ACE_MASK_ATTR_READ)
	AclFileRead = int(C.OFAPI_ACE_MASK_FILE_DATA_READ | C.OFAPI_ACE_MASK_XATTR_READ |
		C.OFAPI_ACE_MASK_ATTR_READ)
	AclDirWrite = int(C.OFAPI_ACE_MASK_DIR_ADD_FILE | C.OFAPI_ACE_MASK_DIR_ADD_DIR |
		C.OFAPI_ACE_MASK_XATTR_WRITE | C.OFAPI_ACE_MASK_ATTR_WRITE |
		C.OFAPI_ACE_MASK_DELETE_CHILD | C.OFAPI_ACE_MASK_DELETE)
	AclFileWrite = int(C.OFAPI_ACE_MASK_FILE_DATA_WRITE | C.OFAPI_ACE_MASK_XATTR_WRITE |
		C.OFAPI_ACE_MASK_ATTR_WRITE | C.OFAPI_ACE_MASK_FILE_DATA_APPEND |
		C.OFAPI_ACE_MASK_DELETE)
	AclRead           = int(C.OFAPI_ACE_MASK_ACL_READ)
	AclWrite          = int(C.OFAPI_ACE_MASK_ACL_WRITE)
	AclDirFullControl = AclDirRead | AclDirWrite | AclWrite | AclRead |
		int(C.OFAPI_ACE_MASK_CHANGE_OWNER|C.OFAPI_ACE_MASK_SYNC|
			C.OFAPI_ACE_MASK_EXECUTE)
	AclFileFullControl = AclFileRead | AclFileWrite | AclWrite | AclRead |
		int(C.OFAPI_ACE_MASK_CHANGE_OWNER|C.OFAPI_ACE_MASK_SYNC|
			C.OFAPI_ACE_MASK_EXECUTE)
	AclFlagDefault = int(C.OFAPI_ACE_FLAG_NO_PROPAGET)
	AclFlagInherit = int(C.OFAPI_ACE_FLAG_FILE_INHERIT |
		C.OFAPI_ACE_FLAG_DIR_INHERIT)
	AclFlagOnlyInherit = int(C.OFAPI_ACE_FLAG_FILE_INHERIT |
                C.OFAPI_ACE_FLAG_DIR_INHERIT | C.OFAPI_ACE_FLAG_INHERIT_ONLY)
	AclExcute = int(C.OFAPI_ACE_MASK_EXECUTE)
)

//for acl operate
type AclGrant struct {
	Uid     int
	Gid     int
	Acltype string
	Aclbits int
	Aclflag int
}

func toCPoint(as []C.struct_oace) *C.struct_oace {
	asHdr := (*reflect.SliceHeader)(unsafe.Pointer(&as))
	return (*C.struct_oace)(unsafe.Pointer(uintptr(asHdr.Data)))
}

const (
	//grantee type for user
	UserType = "CanonicalUser"
	//grantee type for group
	GroupType = "Group"
	//grantee type for owner canned acl
	OwnerType = "Owner"
	//grantee type for every one canned acl
	EveryType = "EveryOne"
)

func SetAcl(path string, grants []AclGrant) error {
	oa := make([]C.struct_oace, 0, len(grants))
	for _, og := range grants {
		var a C.struct_oace
		a.oe_type = C.OFAPI_ACE_TYPE_ALLOWED
		switch og.Acltype {
		case UserType:
			a.oe_credtype = C.OFAPI_ACE_CRED_UID
			a.oe_cred = C.uint32_t(og.Uid)
		case GroupType:
			a.oe_credtype = C.OFAPI_ACE_CRED_GID
			a.oe_cred = C.uint32_t(og.Gid)
		case OwnerType:
			a.oe_credtype = C.OFAPI_ACE_CRED_OWNER
			a.oe_cred = C.uint32_t(og.Uid)
		case EveryType:
			a.oe_credtype = C.OFAPI_ACE_CRED_EVERYONE
		default:
			return syscall.Errno(95)
		}
		a.oe_mask = C.uint32_t(og.Aclbits)
		a.oe_flag = C.uint32_t(og.Aclflag)
		oa = append(oa, a)
	}
	var aid C.uint64_t
	ret := C.ofapi_acladd(root.fs, toCPoint(oa), C.uint32_t(len(oa)), &aid)
	if ret != cok {
		return opfsErr(ret)
	}
	fd, err := open(path)
	defer C.ofapi_close(fd)
	if err != nil {
		log.Printf("open fail %v, %v", path, err)
		return err
	}
	ret = C.ofapi_setattr(fd, C.uint32_t(C.OFAPI_UID_INVAL), C.uint32_t(C.OFAPI_GID_INVAL), C.uint32_t(C.OFAPI_MOD_INVAL), aid)
	if ret != cok {
		log.Printf("setattr failed %v", ret)
		return opfsErr(ret)
	}
	return nil
}

func SetPermission(path string, perm os.FileMode) error {
	fd, err := open(path)
	defer C.ofapi_close(fd)
	if err != nil {
		return err
	}
	ret := C.ofapi_setattr(fd, C.uint32_t(C.OFAPI_UID_INVAL), C.uint32_t(C.OFAPI_GID_INVAL),
			      C.uint32_t(uint32(perm)), C.uint64_t(C.OFAPI_AID_INVAL))
	if ret != cok {
		return opfsErr(ret)
	}

	return nil
}

func SetOwner(path string, uid, gid int) error {
	fd, err := open(path)
	defer C.ofapi_close(fd)
	if err != nil {
		return err
	}
	var cgid, cuid C.uint32_t
	if gid == -1 {
		cgid = C.uint32_t(C.OFAPI_GID_INVAL)
	} else {
		cgid = C.uint32_t(uint32(gid))
	}
	if uid == -1 {
		cuid = C.uint32_t(C.OFAPI_UID_INVAL)
	} else {
		cuid = C.uint32_t(uint32(uid))
	}
	ret := C.ofapi_setattr(fd, cuid, cgid, C.uint32_t(C.OFAPI_MOD_INVAL), C.uint64_t(C.OFAPI_AID_INVAL))
	if ret != cok {
		return opfsErr(ret)
	}

	return nil
}

func (csoa *C.struct_oace) toSlice(slen int) []C.struct_oace {
	var as []C.struct_oace
	asHdr := (*reflect.SliceHeader)(unsafe.Pointer(&as))
	asHdr.Data = uintptr(unsafe.Pointer(csoa))
	asHdr.Len = slen
	asHdr.Cap = slen
	return as
}

func GetAcl(path string) ([]AclGrant, error) {
	fd, err := open(path)
	if err != nil {
		return []AclGrant{}, err
	}

	var oace *C.struct_oace
	var acecnt C.uint32_t
	ret := C.ofapi_aclqry(fd, &oace, &acecnt)
	if ret != cok {
		return []AclGrant{}, opfsErr(ret)
	}

	oa := oace.toSlice(int(acecnt))
	opfsgrants := make([]AclGrant, 0, int(acecnt))
	for i := 0; i < int(acecnt); i++ {
		if oa[i].oe_type == C.OFAPI_ACE_TYPE_DENIED {
			continue
		}
		var og AclGrant
		switch oa[i].oe_credtype {
		case C.OFAPI_ACE_CRED_UID:
			og.Acltype = UserType
			og.Uid = int(oa[i].oe_cred)
		case C.OFAPI_ACE_CRED_GID:
			og.Acltype = GroupType
			og.Gid = int(oa[i].oe_cred)
		case C.OFAPI_ACE_CRED_OWNER:
			og.Acltype = OwnerType
			og.Uid = int(oa[i].oe_cred)
		case C.OFAPI_ACE_CRED_EVERYONE:
			og.Acltype = EveryType
		default:
			og.Acltype = ""
		}
		if og.Acltype == "" {
			continue
		}
		og.Aclbits = int(oa[i].oe_mask)
		og.Aclflag = int(oa[i].oe_flag)
		opfsgrants = append(opfsgrants, og)
	}
	return opfsgrants, nil
}

func GetOwner(path string) (uid, gid int, err error) {
	fd, err := open(path)
	if err != nil {
		return 0, 0, err
	}
	defer C.ofapi_close(fd)

	var oatt C.struct_oatt

	ret := C.ofapi_getattr(fd, &oatt)
	if ret != cok {
		return 0, 0, opfsErr(ret)
	}
	return int(oatt.oa_uid), int(oatt.oa_gid), nil
}
func MakeDirAll(path string, perm os.FileMode) error {
	ipaths := strings.Split(path, SlashSeparator)
	var p strings.Builder
	p.WriteString(root.rootpath)
	for _, s := range ipaths {
		dirPath := p.String() + SlashSeparator + s
		dirPath = pathutils.Clean(dirPath)
		if dirPath == "/" {
			continue
		}
		err := MakeDir(dirPath, perm)
		if err != nil && !errors.Is(err, os.ErrExist) {
			log.Printf("dirPath %v, err %v", dirPath, err)
			return err
		}
		p.WriteString(fmt.Sprintf("/%s", s))
	}

	return nil

}

type FsInfo struct {
	Capacity              uint64
        Used                  uint64
        Remaining             uint64
        UnderReplicated       uint64
        CorruptBlocks         uint64
        MissingBlocks         uint64
        MissingReplOneBlocks  uint64
        BlocksInFuture        uint64
        PendingDeletionBlocks uint64
}

func StatFs() (*FsInfo, error) {
	res := new(FsInfo)
	fd, err := open(root.rootpath)
	if err != nil {
		return res, err
	}
	var fsstat C.struct_statvfs
	ret := C.ofapi_statvfs(fd, &fsstat)
	if ret != cok {
		return res, opfsErr(ret)
	}
	bsize := fsstat.f_bsize
	if fsstat.f_bsize != fsstat.f_frsize || fsstat.f_frsize == C.ulong(0) {
		log.Printf("openfs hdfs will use f_frsize %v %v\n", fsstat.f_bsize, fsstat.f_frsize)
		bsize = fsstat.f_frsize
	}

	res.Capacity = uint64(bsize * fsstat.f_blocks)
	res.Used = uint64(bsize * (fsstat.f_blocks - fsstat.f_bfree))
	res.Remaining = uint64(bsize * fsstat.f_bfree)

	return res, nil
}

type OpfsXAttrEntry struct {
	Scope string
	Name string
	Value []byte
}

func GetXAttr(src string) ([]OpfsXAttrEntry, error) {
	fd, err := open(src)
	if err != nil {
		return nil, err
	}

	var oxae *C.struct_oxattrent

	ret := C.ofapi_getxattr(fd, &oxae)
	if ret != cok {
		return []OpfsXAttrEntry{}, opfsErr(ret)
	}

	if oxae == nil {
		log.Printf("no extend attr")
		return []OpfsXAttrEntry{}, nil
	}

	log.Printf("oxae %T, %v", oxae, oxae)

	res := make([]OpfsXAttrEntry, 0, 128)

	for oxae.ox_size != C.uint32_t(0) {
		conName := C.GoString(oxae.ox_name)
		names := strings.SplitN(conName, ".", 2)
		dataLen := int(oxae.ox_dlen)
		v := make([]byte, dataLen)
		cvalue := (*[1<<30]byte)(unsafe.Pointer(oxae.ox_data))[:dataLen:dataLen]
		copy(v, cvalue)
		e := OpfsXAttrEntry {
			Scope: names[0],
			Name: names[1],
			Value:v,
		}
		res = append(res, e)
		p := unsafe.Pointer(oxae)
		log.Printf("p %T %p", p, p)
		oxae = (*C.struct_oxattrent)(unsafe.Pointer(uintptr(p) + uintptr(oxae.ox_size)))
		log.Printf("oxae %p", oxae)
	}

	return res, nil
}



func SetXAttr(src string, xattr OpfsXAttrEntry) error {
	fd, err := open(src)
	if err != nil {
		return err
	}
	defer C.ofapi_close(fd)
	var cx C.struct_oxattrent
	cx.ox_size = C.uint32_t(0)
	cx.ox_dlen = C.uint32_t(len(xattr.Value))
	cname := C.CString(xattr.Scope+"."+xattr.Name)
	cx.ox_name = cname
	defer C.free(unsafe.Pointer(cname))
	cdata := C.CBytes(xattr.Value)
	defer C.free(unsafe.Pointer(cdata))
	cx.ox_data = cdata
	ret := C.ofapi_setxattr(fd, &cx)
	if ret != cok {
		return opfsErr(ret)
	}

	return nil
}

func RemoveXAttr(src, name string) error {
	fd, err := open(src)
	if err != nil {
		return err
	}

	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))

	ret := C.ofapi_rmvxattr(fd, cname)
	if ret != cok {
		return opfsErr(ret)
	}

	return nil
}

type OpfsQuotaEntry struct {
	Spaceconsume uint64
	Spacequota uint64
}

func CQuotaEntrytoArray(entries *C.struct_oquotent, count C.int32_t) []C.struct_oquotent {
	res := make([]C.struct_oquotent, 0)
	sHdr := (*reflect.SliceHeader)(unsafe.Pointer(&res))
	sHdr.Data = uintptr(unsafe.Pointer(entries))
	sHdr.Len = int(count)
	sHdr.Cap = int(count)
	log.Printf("sHdr %v", sHdr)

	return res
}

func GetSpaceQuota(src string) (*OpfsQuotaEntry, error) {
	var entries *C.struct_oquotent
	var count C.int32_t
	ret := C.ofapi_quotaget(root.fs, C.int32_t(C.OFAPI_QUOTA_TYPE_DIR), &entries, &count)
	if ret != cok {
		return nil, opfsErr(ret)
	}
	defer C.free(unsafe.Pointer(entries))
	qs := CQuotaEntrytoArray(entries, count)
	log.Printf("len %v", len(qs))
	for _, q := range qs {
		var fd *C.ofapi_fd_t
		ret = C.ofapi_openbyid(root.fs, q.oq_oid, C.uint64_t(0), &fd)
		if ret != cok {
			return nil, opfsErr(ret)
		}
		var p *C.char
		ret = C.ofapi_ancestry(fd, &p)
		if ret != cok {
			return nil, opfsErr(ret)
		}
		// if free p pointer but will runtime crash
		// C.free(unsafe.Pointer(p))
		name := pathutils.Clean(C.GoString(p))
		if name == src {
			return &OpfsQuotaEntry {
				Spaceconsume:uint64(q.oq_usage),
				Spacequota: uint64(q.oq_limit_hard),
			}, nil
		}
	}

	return nil, syscall.ENOENT
}

func getIno(src string) (C.uint64_t, error) {
	fd, err := open(src)
	defer C.ofapi_close(fd)
	if err != nil {
		return C.uint64_t(0), err
	}
	stat := C.struct_oatt{}
	ret := C.ofapi_getattr(fd, &stat)
	if ret != cok {
		return C.uint64_t(0), opfsErr(ret)
	}
	return stat.oa_ino, nil
}

func getqid() C.uint64_t {
	return C.uint64_t(time.Now().UnixNano())
}

const NeedFix = 0x2

func SetSpaceQuota(src string, e *OpfsQuotaEntry) error {
	/*
	if src == "/" {
		return syscall.ENOTSUP
	}
	*/
	var entries *C.struct_oquotent
	var count C.int32_t
	ret := C.ofapi_quotaget(root.fs, C.int32_t(C.OFAPI_QUOTA_TYPE_DIR), &entries, &count)
	if ret != cok {
		return opfsErr(ret)
	}
	var target C.struct_oquotent
	found := false
	qs := CQuotaEntrytoArray(entries, count)
	for _, q := range qs {
		var fd *C.ofapi_fd_t
		ret = C.ofapi_openbyid(root.fs, q.oq_oid, C.uint64_t(0), &fd)
		if ret != cok {
			return opfsErr(ret)
		}
		var p *C.char
		ret = C.ofapi_ancestry(fd, &p)
		if ret != cok {
			return opfsErr(ret)
		}
		name := pathutils.Clean(C.GoString(p))
		if name == src {
			found = true
			target = q
			break
		}
		/*
		if strings.HasPrefix(name, src) ||
		   strings.HasPrefix(src, name) {
			return syscall.ENOTSUP
		}
		*/
	}
	if !found {
		target = C.struct_oquotent {
			oq_id: getqid(),
			oq_state: C.uint32_t(NeedFix),
			oq_type: C.int32_t(C.OFAPI_QUOTA_TYPE_DIR),
			oq_limit_soft: C.int64_t(e.Spacequota),
			oq_limit_hard: C.int64_t(e.Spacequota),
		}
		var err error
		target.oq_oid, err = getIno(src)
		if err != nil {
			return err
		}
	} else {
		target.oq_limit_soft = C.int64_t(e.Spacequota)
		target.oq_limit_hard = C.int64_t(e.Spacequota)
		target.oq_state |= C.uint32_t(C.OFAPI_QUOTA_TYPE_DIR)
	}

	ret = C.ofapi_quotaset(root.fs, &target)
	if ret != cok {
		log.Printf("fail set %v", ret)
		return opfsErr(ret)
	}

	if target.oq_state & C.uint32_t(NeedFix) != 0 {
		fd, _ := open(src)
		defer C.ofapi_close(fd)
		ret = C.ofapi_quotafix(fd, target.oq_id, C.int32_t(C.OFAPI_QUOTA_TYPE_DIR))
		if ret != cok {
			log.Printf("fix fail %v", ret)
			return opfsErr(ret)
		}
	}

	return nil
}

func MakePath(path string, perm os.FileMode) error {
	dirs := pathutils.Dir(path)
	if err := MakeDirAll(dirs, perm); err != nil {
		return err
	}
	if err := createFile(path, perm); err != nil {
		return err
	}

	return nil
}

const (
	rootDir = "/"
)

func RemovePath(path string) error {
	if err := RemoveFile(path); err != nil {
		return err
	}

	dirs := pathutils.Dir(path)
	for dirs != rootDir {
		if err := RemoveDir(dirs); err != nil {
			if errors.Is(err, syscall.ENOTEMPTY) {
				return nil
			}
			return err
		}
		dirs = pathutils.Dir(dirs)
	}

	return nil
}
