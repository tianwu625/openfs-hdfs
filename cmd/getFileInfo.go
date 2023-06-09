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
	fsys := fi.Sys()
	opfs_stat := fsys.(*opfs.OpfsStat)

	res.FileType = t.Enum()
	res.Path = []byte(src)
	res.Length = proto.Uint64(uint64(fi.Size()))
	res.Owner = proto.String(fmt.Sprintf("%d", opfs_stat.Uid))
	res.Group = proto.String(fmt.Sprintf("%d", opfs_stat.Gid))
	res.ModificationTime = proto.Uint64(uint64(fi.ModTime().UnixMilli()))
	res.AccessTime = proto.Uint64(uint64(opfs_stat.Atime.UnixMilli()))
	res.Permission = new(hdfs.FsPermissionProto)
	res.Permission.Perm = proto.Uint32(uint32(mode.Perm()))
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
