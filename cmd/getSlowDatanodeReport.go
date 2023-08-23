package cmd

import (
	"context"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getSlowDatanodeReportDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetSlowDatanodeReportRequestProto)
	return parseRequest(b, req)
}

func getSlowDatanodeReport(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetSlowDatanodeReportRequestProto)
	return opfsGetSlowDatanodeReport(req)
}

func opfsGetSlowDatanodeReport(r *hdfs.GetSlowDatanodeReportRequestProto)(*hdfs.GetSlowDatanodeReportResponseProto, error) {
	return &hdfs.GetSlowDatanodeReportResponseProto {
		DatanodeInfoProto: []*hdfs.DatanodeInfoProto {
		},
	}, nil
}

