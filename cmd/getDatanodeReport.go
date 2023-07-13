package cmd

import (
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getDatanodeReportDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetDatanodeReportRequestProto)
	return parseRequest(b, req)
}

func getDatanodeReport(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetDatanodeReportRequestProto)
	log.Printf("type %v\n", req.GetType())
	return opfsGetDatanodeReport(req)
}

type opfsDatanodeInfo struct {

}

func filterTypeDatanodeInfo(ntype string) []*opfsDatanodeInfo{
}

func datanodeInfoToProto(infos []*opfsDatanodeInfo) [] {
}

func opfsGetDatanodeReport(r *hdfs.GetDatanodeReportRequestProto) (*hdfs.GetDatanodeReportResponseProto, error) {
	nodetype := r.GetType().String()

	dis := filterTypeDatanodeInfo(nodetype)

	disproto := datanodeInfoToProto(dis)

	return &hdfs.GetDatanodeReportResponseProto {
		Di: disproto,
	}, nil
}
