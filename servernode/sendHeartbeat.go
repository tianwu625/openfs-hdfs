package servernode

import (
	"log"
	"context"

	hdsp "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_server"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/rpc"
)

func sendHeartbeatDec(b []byte) (proto.Message, error) {
	req := new(hdsp.HeartbeatRequestProto)
	return rpc.ParseRequest(b, req)
}

func sendHeartbeat(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdsp.HeartbeatRequestProto)
	log.Printf("req %v", req)
	res, err := opfsSendHeartbeat(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

var idx uint64 = 0
var fullBlockReportLeaseId uint64 = 7050185255286347141

func convertStorageReportProtoToDataStorageInfo (reports []*hdfs.StorageReportProto) []*DataStorageInfo {
	res := make([]*DataStorageInfo, 0, len(reports))
	for _, r := range reports {
		log.Printf("r::::::%v", r)
		d := &DataStorageInfo {
			Uuid: r.GetStorageUuid(),
			State: r.GetStorage().GetState().String(),
			Failed: r.GetFailed(),
			Capacity: r.GetCapacity(),
			DfsUsed: r.GetDfsUsed(),
			Remaining: r.GetRemaining(),
			BlockPoolUsed:r.GetBlockPoolUsed(),
			StorageType:r.GetStorage().GetStorageType().String(),
			NonDfsUsed:r.GetNonDfsUsed(),
		}
		log.Printf("heartbeat:::::type %v", d.StorageType)
		res = append(res, d)
	}
	return res
}

func opfsUpdateDatanodeStorage(reg *hdsp.DatanodeRegistrationProto, reports []*hdfs.StorageReportProto) error {
	datanodemap := GetGlobalDatanodeMap()
	node := datanodemap.getDatanodeEntry(reg.GetDatanodeID().GetDatanodeUuid())
	if node == nil {
		log.Printf("node %v not in datanodeMap??")
		err := datanodemap.Register(convertRegisterProtoToDatanode(reg))
		if err != nil {
			log.Printf("register failed")
			return err
		}
		node = datanodemap.getDatanodeEntry(reg.GetDatanodeID().GetDatanodeUuid())
	}
	info := &DatanodeStorages {
		storages: convertStorageReportProtoToDataStorageInfo(reports),
	}
	if err := node.UpdateStorages(info); err != nil {
		return err
	}

	return nil
}

func opfsSendHeartbeat(r *hdsp.HeartbeatRequestProto) (*hdsp.HeartbeatResponseProto, error) {
	log.Printf("register %v\nreports %v\nxmitsInprogress %v\nxceivercount %v\nfailvolumes %v",
		   r.GetRegistration(), r.GetReports(), r.GetXmitsInProgress(), r.GetXceiverCount(), r.GetFailedVolumes())
	log.Printf("cache %v\ncache use %v\nvolumefailuresummary %v\nreportlease %v\nslowpeers %v\nslowdisk %v",
		   r.GetCacheCapacity(), r.GetCacheUsed(), r.GetVolumeFailureSummary(),
		   r.GetRequestFullBlockReportLease(),r.GetSlowPeers(), r.GetSlowDisks())
	err := opfsUpdateDatanodeStorage(r.GetRegistration(), r.GetReports())
	if err != nil {
		return nil, err
	}
	txid := idx
	fullBlockReportLeaseId++
	fullid := fullBlockReportLeaseId
	resp := &hdsp.HeartbeatResponseProto {
		HaStatus: &hdfs.NNHAStatusHeartbeatProto {
			State: hdfs.NNHAStatusHeartbeatProto_ACTIVE.Enum(),
			Txid: proto.Uint64(txid),
		},
	}
	if r.GetRequestFullBlockReportLease() {
		resp.FullBlockReportLeaseId = proto.Uint64(fullid)
	}

	return resp, nil
}

