package cmd

import (
	"context"
	"log"
	"os"
	"fmt"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/logger"
)

func rename2Dec(b []byte) (proto.Message, error) {
	req := new(hdfs.Rename2RequestProto)
	return parseRequest(b, req)
}

func rename2(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.Rename2RequestProto)
	log.Printf("src %v\ndst %v\nOverwriteDest %v\nMoveToTrash %v\n", req.GetSrc(),
	req.GetDst(), req.GetOverwriteDest(), req.GetMoveToTrash())
	return opfsRename2(ctx, req)
}

func opfsRename2(ctx context.Context, r *hdfs.Rename2RequestProto) (*hdfs.Rename2ResponseProto, error) {
	res := new(hdfs.Rename2ResponseProto)
	src := r.GetSrc()
	dst := r.GetDst()
	overwrite := r.GetOverwriteDest()
	trash := r.GetMoveToTrash()

	f, err := opfs.Open(dst)
	if err == nil && !overwrite {
		f.Close()
		logger.LogIf(ctx, fmt.Errorf("dst %v exist %v", dst, err))
		return res, os.ErrExist
	} else if err != nil && !os.IsNotExist(err) {
		return res, err
	}

	if trash {
		log.Printf("move dst to trash for openfs")
	}

	if err := renameFile(ctx, src, dst); err != nil {
		return res, err
	}

	return res, nil
}
