package cmd

import (
	"context"
	"time"
	"log"
	"math"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func setTimesDec(b []byte) (proto.Message, error) {
	req := new(hdfs.SetTimesRequestProto)
	return parseRequest(b, req)
}

func setTimes(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.SetTimesRequestProto)
	log.Printf("src %v\nmtime %v\natime %v\n", req.GetSrc(), req.GetMtime(), req.GetAtime())
	return opfsSetTimes(req)
}

func opfsSetTimes(r *hdfs.SetTimesRequestProto) (*hdfs.SetTimesResponseProto, error) {
	src := r.GetSrc()
	mtime64 := r.GetMtime()
	atime64 := r.GetAtime()

	log.Printf("time.Now %v", time.Now().UnixMilli())

	setm := false
	seta := false
	var mtime, atime time.Time
	if mtime64 != math.MaxUint64 {
		mtime = time.UnixMilli(int64(mtime64))
		setm = true
	}
	if atime64 != math.MaxUint64 {
		atime = time.UnixMilli(int64(atime64))
		seta = true
	}

	res := new(hdfs.SetTimesResponseProto)
	if err := opfs.Utime(src, mtime, setm, atime, seta); err != nil {
		return res, err
	}

	return res, nil
}
