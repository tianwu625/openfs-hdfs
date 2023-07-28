package cmd

import (
	"log"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func listOpenFilesDec(b []byte) (proto.Message, error) {
	req := new(hdfs.ListOpenFilesRequestProto)
	return parseRequest(b, req)
}

func listOpenFiles(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.ListOpenFilesRequestProto)
	res, err := opfsListOpenFiles(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsListOpenFiles(r *hdfs.ListOpenFilesRequestProto)(*hdfs.ListOpenFilesResponseProto, error) {
	id := r.GetId()
	types := r.GetTypes()
	p := r.GetPath()

	log.Printf("id %v, types %v, path %v", id, types, p)

	entries := make([]*hdfs.OpenFilesBatchResponseProto, 0)
	return &hdfs.ListOpenFilesResponseProto {
		Entries: entries,
		HasMore: proto.Bool(false),
		Types:types,
	}, nil
}
