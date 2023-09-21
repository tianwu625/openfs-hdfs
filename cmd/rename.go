package cmd

import (
	"context"
	"log"
	"os"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/logger"
)

func renameDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RenameRequestProto)
	return parseRequest(b, req)
}

func rename(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RenameRequestProto)
	log.Printf("src %v\ndst %v\n", req.GetSrc(), req.GetDst())
	return opfsRename(ctx, req)
}

func opfsRename(ctx context.Context, r *hdfs.RenameRequestProto) (*hdfs.RenameResponseProto, error) {
	res := new(hdfs.RenameResponseProto)
	res.Result = proto.Bool(false)
	src := r.GetSrc()
	dst := r.GetDst()

	f, err := opfs.Open(dst)
	if err == nil {
		f.Close()
		return res, nil
	} else if err != nil && !os.IsNotExist(err) {
		return res, nil
	}

	if err := renameFile(ctx, src, dst); err != nil {
		if os.IsNotExist(err) {
			logger.LogIf(ctx, err)
			return res, nil
		}
		logger.LogIf(ctx, err)
		return res, err
	}

	res.Result = proto.Bool(true)

	return res, nil
}
