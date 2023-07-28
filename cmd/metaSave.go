package cmd

import (
	"log"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func metaSaveDec(b []byte) (proto.Message, error) {
	req := new(hdfs.MetaSaveRequestProto)
	return parseRequest(b, req)
}

func metaSave(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.MetaSaveRequestProto)
	log.Printf("filename %v", req.GetFilename())
	res, err := opfsMetaSave(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsMetaSave(r *hdfs.MetaSaveRequestProto) (*hdfs.MetaSaveResponseProto,error) {
	file := r.GetFilename()

	log.Printf("filename %v", file)
	/*
	 need to know metad data format,
	 if java struct to binary, go may be can't do the same binary
	 !!! need to confirm this format from java code !!!
	 */

	return new(hdfs.MetaSaveResponseProto), nil
}
