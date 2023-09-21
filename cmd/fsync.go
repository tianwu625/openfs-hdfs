package cmd

import (
	"log"
	"context"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)


func fsyncDec(b []byte) (proto.Message, error) {
	req := new(hdfs.FsyncRequestProto)
	return parseRequest(b, req)
}

func fsync(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.FsyncRequestProto)
	res, err := opfsFsync(ctx, req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsFsync(ctx context.Context, r *hdfs.FsyncRequestProto) (*hdfs.FsyncResponseProto, error) {
	log.Printf("fsync req src %v, client %v, lastblockLength %v, fileId %v",
			r.GetSrc(), r.GetClient(), r.GetLastBlockLength(), r.GetFileId())
	lastLength := r.GetLastBlockLength()
	if lastLength != int64(-1) {
		//check last block status
		log.Printf("should check last block length and status")
	}
	fileId := r.GetFileId()
	if fileId != uint64(0) {
		//check fileId mismatch or not
		log.Printf("should check fileid mismatch or not")
	}

	return &hdfs.FsyncResponseProto{}, nil
}
