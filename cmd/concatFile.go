package cmd

import (
	"log"
	"io"
	"os"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func concatFileDec(b []byte) (proto.Message, error) {
	req := new(hdfs.ConcatRequestProto)
	return parseRequest(b, req)
}

func concatFile(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.ConcatRequestProto)
	log.Printf("dst %v\nsrcs %v\n", req.GetTrg(), req.GetSrcs())
	return opfsConcatFile(req)
}

func appendToFile(w io.Writer, rs ...io.ReadCloser) error {
	for _, r := range rs {
		_, err := io.Copy(w, r)
		if err != nil {
			return err
		}
	}

	return nil
}

func openReaders(srcs ...string) ([]io.ReadCloser, error) {
	res := make([]io.ReadCloser, 0, len(srcs))
	for _, src := range srcs {
		f, err := opfs.Open(src)
		if err != nil {
			return res, err
		}

		res = append(res, f)
	}

	return res, nil
}

func closeReader(closers ...io.ReadCloser) {
	for _, closer := range closers {
		closer.Close()
	}
}

func removeReader(srcs ...string) error {
	for _, src := range srcs {
		if err := opfs.RemoveFile(src); err != nil {
			return err
		}
	}

	return nil
}


func opfsConcatFile(r *hdfs.ConcatRequestProto) (*hdfs.ConcatResponseProto, error) {
	res := new(hdfs.ConcatResponseProto)
	dst := r.GetTrg()
	srcs := r.GetSrcs()

	fw, err := opfs.OpenWithCreate(dst, os.O_WRONLY|os.O_APPEND, os.FileMode(0))
	if err != nil {
		return res, err
	}
	readclosers, err := openReaders(srcs...)
	if err != nil {
		return res, err
	}
	defer closeReader(readclosers...)
	err = appendToFile(fw, readclosers...)
	if err != nil {
		return res, err
	}

	err = removeReader(srcs...)
	if err != nil {
		return res, err
	}

	return res, nil
}
