package cmd

import (
	"log"
	"errors"
	"os"
	"io"
	"path"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

var errNotFound = errors.New("not found this entry")
var errNotSupport = errors.New("not support parameter")

func opfsGetLastPos(entries []os.FileInfo, last string) (int, error) {
	for i, e := range entries {
		if e.Name() == last {
			return i, nil
		}
	}

	return -1, errNotFound
}

func opfsGetListing(r *hdfs.GetListingRequestProto) (proto.Message, error) {
	src := r.GetSrc()
	last := (string)(r.GetStartAfter())
	needlocal := r.GetNeedLocation()
	res := new(hdfs.GetListingResponseProto)
	if needlocal {
		return res, errNotSupport
	}

	f, err := opfs.Open(src)
	if err != nil {
		log.Printf("open %v fail %v\n", src, err)
		return res, err
	}

	entries, err := f.Readdir(-1)
	if err != nil && !errors.Is(err, io.EOF) {
		log.Printf("readdir %v fail %v\n", src, err)
	}
	if last != "" {
		n, err := opfsGetLastPos(entries, last)
		if err != nil {
			return res, err
		}
		entries = entries[n+1:]
	}
	res.DirList = new(hdfs.DirectoryListingProto)
	dlist := make([]*hdfs.HdfsFileStatusProto, 0, len(entries))
	for _, e := range entries {
		d := opfsHdfsFileStatus(path.Join(src, e.Name()), e, nil)
		dlist = append(dlist, d)
	}
	res.DirList.PartialListing = dlist
	res.DirList.RemainingEntries = proto.Uint32(0)

	return res, nil
}
