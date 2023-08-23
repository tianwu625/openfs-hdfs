package cmd

import (
	"log"
	"path"
	"errors"
	"io"
	"os"
	"context"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getSnapshottableDirListingDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetSnapshottableDirListingRequestProto)
	return parseRequest(b, req)
}

func getSnapshottableDirListing(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetSnapshottableDirListingRequestProto)
	res, err := opfsGetSnapshottableDirListing(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func getSnapCount(src string) (uint32, error) {
	readdirSrc := ""
	if src == "/" {
		readdirSrc = path.Join("/", hdfsRootSnapDir)
	} else {
		readdirSrc = path.Join(src, snapshotDir)
	}
	f, err := opfs.Open(readdirSrc)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("open %v fail %v\n", readdirSrc, err)
			return 0, err
		}
		return 0, nil
	}
	defer f.Close()

	entries, err := f.Readdir(-1)
	if err != nil && !errors.Is(err, io.EOF) {
		log.Printf("readdir %v fail %v\n", src, err)
		return 0, err
	}

	return uint32(len(entries)), nil
}

func opfsGetSnapshottableDirListing(r *hdfs.GetSnapshottableDirListingRequestProto)(*hdfs.GetSnapshottableDirListingResponseProto, error) {
	dlist := make([]*hdfs.SnapshottableDirectoryStatusProto, 0)
	src := "/"
	status, err := opfsHdfsStat(src)
	if err != nil {
		return nil, err
	}
	count, err := getSnapCount(src)
	if err != nil {
		return nil, err
	}

	d := &hdfs.SnapshottableDirectoryStatusProto {
		DirStatus: status,
		SnapshotQuota: proto.Uint32(65536),
		SnapshotNumber: proto.Uint32(count),
		ParentFullpath: []byte("/"),
	}
	dlist = append(dlist, d)

	return &hdfs.GetSnapshottableDirListingResponseProto {
		SnapshottableDirList: &hdfs.SnapshottableDirectoryListingProto {
			SnapshottableDirListing: dlist,
		},
	}, nil
}
