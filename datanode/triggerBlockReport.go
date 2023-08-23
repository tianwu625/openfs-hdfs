package datanode

import (
	"log"
	"context"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func triggerBlockReportDec(b []byte) (proto.Message, error) {
	req := new(hdfs.TriggerBlockReportRequestProto)
	return parseRequest(b, req)
}

func triggerBlockReport(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.TriggerBlockReportRequestProto)
	res, err := opfsTriggerBlockReportRequest(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsTriggerBlockReportRequest(r *hdfs.TriggerBlockReportRequestProto) (*hdfs.TriggerBlockReportResponseProto, error) {
	incre := r.GetIncremental()
	namenode := r.GetNnAddress()

	log.Printf("incremetal %v, namenode %v", incre, namenode)

	return new(hdfs.TriggerBlockReportResponseProto), nil
}
