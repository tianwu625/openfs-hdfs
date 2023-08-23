package cmd

import (
	"context"
	"log"
	iofs "io/fs"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func mkdirsDec(b []byte) (proto.Message, error) {
	req := new(hdfs.MkdirsRequestProto)
	return parseRequest(b, req)
}

func mkdirs(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.MkdirsRequestProto)
	log.Printf("src %v\nmasked %o\ncreateparent %v\numask %o\n", req.GetSrc(),
	req.GetMasked().GetPerm(), req.GetCreateParent(), req.GetUnmasked().GetPerm())
	return opfsMkdirs(req)
}

const allperm = 0777

func opfsMkdirs(r *hdfs.MkdirsRequestProto) (*hdfs.MkdirsResponseProto, error) {
	res := new(hdfs.MkdirsResponseProto)
	res.Result = proto.Bool(false)

	src := r.GetSrc()
	mask := r.GetMasked().GetPerm() & allperm
	if mask == 0 {
		mask = ^(r.GetUnmasked().GetPerm()) & allperm
	}
	log.Printf("mask %o\n", mask)
	all := r.GetCreateParent()

	if all {
		err := opfs.MakeDirAll(src, (iofs.FileMode)(mask))
		if err != nil {
			return res, err
		}
	} else {
		err := opfs.MakeDir(src, (iofs.FileMode)(mask))
		if err != nil {
			return res, err
		}
	}

	res.Result = proto.Bool(true)

	return res, nil
}
