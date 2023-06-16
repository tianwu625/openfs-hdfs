package cmd

import (
	"log"
	"os"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func renameDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RenameRequestProto)
	return parseRequest(b, req)
}

func rename(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RenameRequestProto)
	log.Printf("src %v\ndst %v\n", req.GetSrc(), req.GetDst())
	return opfsRename(req)
}

func opfsRename(r *hdfs.RenameRequestProto) (*hdfs.RenameResponseProto, error) {
	res := new(hdfs.RenameResponseProto)
	res.Result = proto.Bool(false)
	src := r.GetSrc()
	dst := r.GetDst()

	f, err := opfs.Open(dst)
	if err == nil {
		f.Close()
		return res, os.ErrExist
	} else if err != nil && !os.IsNotExist(err) {
		return res, err
	}

	err = opfs.Rename(src, dst)
	if err != nil {
		return res, err
	}
	res.Result = proto.Bool(true)

	return res, nil
}
