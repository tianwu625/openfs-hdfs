package cmd

import (
	"log"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func saveNamespaceDec(b []byte) (proto.Message, error) {
	req := new(hdfs.SaveNamespaceRequestProto)
	return parseRequest(b, req)
}

func saveNamespace(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.SaveNamespaceRequestProto)
	log.Printf("timewindows %v, txgap %v", req.GetTimeWindow(), req.GetTxGap())
	return opfsSaveNamespace(req)
}

func opfsSaveNamespace(r *hdfs.SaveNamespaceRequestProto) (*hdfs.SaveNamespaceResponseProto, error) {
	twindow := r.GetTimeWindow()
	gap := r.GetTxGap()

	log.Printf("twindow %v, gap %v", twindow, gap)

	return &hdfs.SaveNamespaceResponseProto {
		Saved: proto.Bool(true),
	}, nil
}
