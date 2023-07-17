package cmd

import (
	"log"
	"os"

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

type opfsDatanodeMeta struct {
	IpAddr string
	Hostname string
	Uuid string
	XferPort uint32
	InfoPort uint32
	IpcPort uint32
	InfoSecurePort uint32
}

type opfsDatanodeStats struct {
	Capacity uint64
	DfsUsed uint64
	Remaining uint64
	BlockPoolUsed uint64
	LastUpdate uint64
	XceiverCount uint32
	Location string
	NonDfsUsed uint64
	AdminState string
	CacheCapacity uint64
	CacheUsed uint64
	LastUpdateMonotonic uint64
	LastBlockReportTime uint64
	LastBlockReportMonotonic uint64
	NumBlocks uint32
}

type opfsDatanodeInfo struct {
	info *opfsDatanodeMeta
	stats *opfsDatanodeStats
}

func filterTypeDatanodeInfo(ntype string) []*opfsDatanodeInfo{
	log.Printf("type %v", ntype)
	res := make([]*opfsDatanodeInfo, 0, 1)

	hostname, _ := os.Hostname()
	r := &opfsDatanodeInfo{
		info: &opfsDatanodeMeta {
			IpAddr: "127.0.0.1",
			Hostname: hostname,
			Uuid: hostname+ "-" + "127.0.0.1",
			XferPort: defaultDataNodeXferPort,
			InfoPort: 50075,
			IpcPort: 50020,
			InfoSecurePort: 50475,
		},
	}

	res = append(res, r)


	return res
}

func metaToProto(info *opfsDatanodeMeta) *hdfs.DatanodeIDProto {
	return &hdfs.DatanodeIDProto {
		IpAddr: proto.String(info.IpAddr),
		HostName: proto.String(info.Hostname),
		DatanodeUuid: proto.String(info.Uuid),
		XferPort:proto.Uint32(info.XferPort),
		InfoPort:proto.Uint32(info.InfoPort),
		IpcPort:proto.Uint32(info.IpcPort),
		InfoSecurePort:proto.Uint32(info.InfoSecurePort),
	}
}

func datanodeInfoToProto(infos []*opfsDatanodeInfo) []*hdfs.DatanodeInfoProto{
	res := make([]*hdfs.DatanodeInfoProto, 0, len(infos))
	for _, info := range infos {
		r := &hdfs.DatanodeInfoProto {
			Id: metaToProto(info.info),
		}
		res = append(res, r)
	}

	return res
}

func opfsGetDatanodeReport(r *hdfs.GetDatanodeReportRequestProto) (*hdfs.GetDatanodeReportResponseProto, error) {
	nodetype := r.GetType().String()

	dis := filterTypeDatanodeInfo(nodetype)

	disproto := datanodeInfoToProto(dis)

	return &hdfs.GetDatanodeReportResponseProto {
		Di: disproto,
	}, nil
}
