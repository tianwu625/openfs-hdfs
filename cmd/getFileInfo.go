package cmd

import (
	"fmt"
	iofs "io/fs"
	"log"
	"os"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

const ProtoFlagNone uint32 = 0

const (
	ProtoFlagAcl uint32 = (1 << iota)
	ProtoFlagCrypt
	ProtoFlagEc
	ProtoFlagSnapshot
)

func getFileInfoDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetFileInfoRequestProto)
	return parseRequest(b, req)
}

func getFileInfo(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetFileInfoRequestProto)
	log.Printf("src %v\n", req.GetSrc())
	return opfsGetFileInfo(req)
}

func opfsgetSysInfo(fi os.FileInfo) (*opfs.OpfsStat, error) {
	fsys := fi.Sys()
	opfsStat, ok := fsys.(*opfs.OpfsStat)
	if !ok {
		return nil, fmt.Errorf("sysInfo not opfsStat")
	}

	return opfsStat, nil
}

func opfsGetSysInfo(src string) (*opfs.OpfsStat, error) {
	f, err := opfs.Open(src)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return opfsgetSysInfo(fi)
}

func opfsHdfsFileStatus(src string, fi os.FileInfo, res *hdfs.HdfsFileStatusProto) *hdfs.HdfsFileStatusProto {
	if res == nil {
		res = new(hdfs.HdfsFileStatusProto)
	}
	mode := fi.Mode()
	t := new(hdfs.HdfsFileStatusProto_FileType)
	if mode.IsDir() {
		*t = hdfs.HdfsFileStatusProto_IS_DIR
	} else if mode.IsRegular() {
		*t = hdfs.HdfsFileStatusProto_IS_FILE
	} else if mode&iofs.ModeSymlink != 0 {
		*t = hdfs.HdfsFileStatusProto_IS_SYMLINK
	} else {
		panic(fmt.Sprintf("file type not support %v", mode))
	}
	opfs_stat, _ := opfsgetSysInfo(fi)

	perm, setAcl, _ := opfsGetPermWithAcl(src)

	log.Printf("getFileInfo perm %o", perm)
	flag := ProtoFlagNone
	if setAcl {
		flag |= ProtoFlagAcl
	}

	res.FileType = t.Enum()
	res.Path = []byte(src)
	res.Length = proto.Uint64(uint64(fi.Size()))
	res.Owner = proto.String(fmt.Sprintf("%d", opfs_stat.Uid))
	res.Group = proto.String(fmt.Sprintf("%d", opfs_stat.Gid))
	res.ModificationTime = proto.Uint64(uint64(fi.ModTime().UnixMilli()))
	res.AccessTime = proto.Uint64(uint64(opfs_stat.Atime.UnixMilli()))
	res.Permission = &hdfs.FsPermissionProto {
		Perm: proto.Uint32(perm),
	}
	res.Blocksize = proto.Uint64(128*1024*1024)
	res.FileId = proto.Uint64(opfs_stat.Ino)
	res.Flags = proto.Uint32(1)
	log.Printf("fs %v\n", res)
	return res
}

func opfsHdfsStat(src string) (*hdfs.HdfsFileStatusProto, error) {
	res := new(hdfs.HdfsFileStatusProto)
	f, err := opfs.Open(src)
	if err != nil {
		log.Printf("fail open %v\n", err)
		return res, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return res, err
	}
	res = opfsHdfsFileStatus(src, fi, res)
	return res, nil
}

func opfsGetFileInfo(r *hdfs.GetFileInfoRequestProto) (*hdfs.GetFileInfoResponseProto, error) {
	src := r.GetSrc()
	res := new(hdfs.GetFileInfoResponseProto)
	fs, err := opfsHdfsStat(src)
	if err != nil{
		if os.IsNotExist(err) {
			return res, nil
		}
		return res, err
	}
	res.Fs = fs
	return res, nil
}
