package servernode

import (
	"log"

	hdsp "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_server"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/rpc"
)

func sendHeartbeatDec(b []byte) (proto.Message, error) {
	req := new(hdsp.HeartbeatRequestProto)
	return rpc.ParseRequest(b, req)
}

func sendHeartbeat(m proto.Message) (proto.Message, error) {
	req := m.(*hdsp.HeartbeatRequestProto)

	res, err := opfsSendHeartbeat(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

var idx uint64 = 0

func opfsSendHeartbeat(r *hdsp.HeartbeatRequestProto) (*hdsp.HeartbeatResponseProto, error) {
	log.Printf("register %v\nreports %v\nxmitsInprogress %v\nxceivercount %v\nfailvolumes %v",
		   r.GetRegistration(), r.GetReports(), r.GetXmitsInProgress(), r.GetXceiverCount(), r.GetFailedVolumes())
	log.Printf("cache %v\ncache use %v\nvolumefailuresummary %v\nreportlease %v\nslowpeers %v\nslowdisk %v",
		   r.GetCacheCapacity(), r.GetCacheUsed(), r.GetVolumeFailureSummary(),
		   r.GetRequestFullBlockReportLease(),r.GetSlowPeers(), r.GetSlowDisks())
	idx++
	return &hdsp.HeartbeatResponseProto {
		HaStatus: &hdfs.NNHAStatusHeartbeatProto {
			State: hdfs.NNHAStatusHeartbeatProto_ACTIVE.Enum(),
			Txid: proto.Uint64(idx),
		},
	}, nil
}

