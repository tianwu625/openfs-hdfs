package cmd

import (
	"log"
	"os"
	"errors"
	"context"
	"fmt"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/logger"
)

var errNotEmpty = errors.New("Not empty directory")

func deleteFileDec(b []byte) (proto.Message, error) {
	req := new(hdfs.DeleteRequestProto)
	return parseRequest(b, req)
}

func deleteFile(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.DeleteRequestProto)
	log.Printf("src %v\nrecursive %v\n", req.GetSrc(), req.GetRecursive())
	res, err := opfsDeleteFile(ctx, req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsDeleteFile(ctx context.Context, r *hdfs.DeleteRequestProto) (*hdfs.DeleteResponseProto, error) {
	res := new(hdfs.DeleteResponseProto)
	res.Result = proto.Bool(false)
	src := r.GetSrc()
	recursive := r.GetRecursive()
	f, err := opfs.Open(src)
	if err != nil {
		if os.IsNotExist(err) {
			return res, nil
		}
		log.Printf("fail open %v\n", err)
		return res, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		log.Printf("fail to Stat %v\n", err)
		return res, err
	}

	if recursive {
		err := removeAllPath(src)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("fail to remove all path %v\n", err))
			return res, err
		}
	} else if !recursive && fi.IsDir() {
		err := removeDir(src)
		if err != nil {
			return res, err
		}
	} else if !recursive && !fi.IsDir() {
		err := removeFile(src)
		if err != nil {
			return res, err
		}
	}
	res.Result = proto.Bool(true)
	return res, nil
}
