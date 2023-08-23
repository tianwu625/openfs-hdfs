package cmd

import (
	"log"
	"errors"
	"os"
	"io"
	"path"
	"fmt"
	"context"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/logger"
)

var errNotFound = errors.New("not found this entry")
var errNotSupport = errors.New("not support parameter")

func getListingDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetListingRequestProto)
	return parseRequest(b, req)
}

func getListing(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetListingRequestProto)
	log.Printf("src %v\nstart %v\nlocal%v\n", req.GetSrc(), req.GetStartAfter(), req.GetNeedLocation())
	return opfsGetListing(req)
}

func opfsGetLastPos(entries []os.FileInfo, last string) (int, error) {
	for i, e := range entries {
		if e.Name() == last {
			return i, nil
		}
	}

	return -1, errNotFound
}

const (
	hdfsRootSnapDir = ".hdfs.snapshot"
)

func filterSysDir(entries []os.FileInfo) []os.FileInfo {
	log.Printf("entries %v", entries)
outer:
	for {
		for i, e := range entries {
			if e.Name() == hdfsSysDirName {
				entries = append(entries[:i], entries[i+1:]...)
				continue outer
			}
			if e.Name() == hdfsRootSnapDir {
				entries = append(entries[:i], entries[i+1:]...)
				continue outer
			}
		}
		break
	}

	return entries
}

const (
	snapshotDir = ".snapshot"
	rootDir = "/"
)

func specialProccessReserveDirs(src string, dlist []*hdfs.HdfsFileStatusProto) []*hdfs.HdfsFileStatusProto {
	if path.Base(src) == snapshotDir {
		replaceSnapDir := path.Dir(src) == rootDir
		for _, d := range dlist {
			if replaceSnapDir {
				d.Path = []byte(path.Join(rootDir, snapshotDir, path.Base(string(d.Path))))
			}
			d.Length = proto.Uint64(0)
		}
	}

	return dlist
}

func opfsGetListing(r *hdfs.GetListingRequestProto) (proto.Message, error) {
	src := r.GetSrc()
	last := (string)(r.GetStartAfter())
	needlocal := r.GetNeedLocation()
	res := new(hdfs.GetListingResponseProto)
	if needlocal {
		return res, errNotSupport
	}

	readdirSrc := src
	//process snapshot name is different for hdfs and openfs
	if src == path.Join("/", snapshotDir) {
		readdirSrc = path.Join("/", hdfsRootSnapDir)
	}

	f, err := opfs.Open(readdirSrc)
	if err != nil {
		log.Printf("open %v fail %v\n", src, err)
		return res, err
	}
	defer f.Close()

	entries, err := f.Readdir(-1)
	if err != nil && !errors.Is(err, io.EOF) {
		log.Printf("readdir %v fail %v\n", src, err)
	}
	//filter .hdfs.sys dir 
	if src == "/" {
		entries = filterSysDir(entries)
	}
	if last != "" {
		n, err := opfsGetLastPos(entries, last)
		if err != nil {
			return res, err
		}
		entries = entries[n+1:]
	}
	logger.LogIf(nil, fmt.Errorf("test trace info"))
	res.DirList = new(hdfs.DirectoryListingProto)
	dlist := make([]*hdfs.HdfsFileStatusProto, 0, len(entries))
	for _, e := range entries {
		d := opfsHdfsFileStatus(path.Join(readdirSrc, e.Name()), e, nil)
		dlist = append(dlist, d)
	}
	res.DirList.PartialListing = specialProccessReserveDirs(src, dlist)
	res.DirList.RemainingEntries = proto.Uint32(0)

	return res, nil
}
