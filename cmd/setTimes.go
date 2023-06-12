package cmd

import (
	"time"
	"log"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func setTimesDec(b []byte) (proto.Message, error) {
	req := new(hdfs.SetTimesRequestProto)
	return parseRequest(b, req)
}

func setTimes(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.SetTimesRequestProto)
	log.Printf("src %v\nmtime %v\natime %v\n", req.GetSrc(), req.GetMtime(), req.GetAtime())
	return opfsSetTimes(req)
}

func opfsSetTimes(r *hdfs.SetTimesRequestProto) (*hdfs.SetTimesResponseProto, error) {
	src := r.GetSrc()
	mtime64 := int64(r.GetMtime())
	atime64 := int64(r.GetAtime())

	mtime := time.UnixMilli(mtime64)
	atime := time.UnixMilli(atime64)

	res := new(hdfs.SetTimesResponseProto)
	if err := opfs.Utime(src, mtime, atime); err != nil {
		return res, err
	}

	return res, nil
}
