package servernode

import (
	"log"
	"context"
	"net"

	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/rpc"
	hdsp "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_server"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"github.com/openfs/openfs-hdfs/internal/logger"
)

func registerDatanodeDec(b []byte) (proto.Message, error) {
	req := new(hdsp.RegisterDatanodeRequestProto)
	return rpc.ParseRequest(b, req)
}

func registerDatanodeUpdateParams(ctx context.Context, r *hdsp.RegisterDatanodeRequestProto) context.Context {
	reg := r.GetRegistration()
	reqInfo := logger.GetReqInfo(ctx)
	if reqInfo == nil {
		return ctx
	}
	reqInfo.SetParams("registration", reg)
	return logger.SetReqInfo(ctx, reqInfo)
}

func registerDatanode(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdsp.RegisterDatanodeRequestProto)
	ctx = registerDatanodeUpdateParams(ctx, req)
	res, err := opfsRegisterDatanode(ctx, req)
	if err != nil {
		return nil, err
	}

	return res, nil
}


func DatanodeIDProtoToDatanodeID(id *hdfs.DatanodeIDProto) Datanodeid {
	return Datanodeid {
		Ipaddr: id.GetIpAddr(),
		Hostname: id.GetHostName(),
		Uuid: id.GetDatanodeUuid(),
		Xferport: id.GetXferPort(),
		Infoport: id.GetInfoPort(),
		Ipcport: id.GetIpcPort(),
		Infosecureport: id.GetInfoSecurePort(),
	}
}

func StorageInfoProtoToStorageInfo(info *hdfs.StorageInfoProto) StorageInfo {
	return StorageInfo {
		LayoutVersion: info.GetLayoutVersion(),
		NamespaceId:info.GetNamespceID(),
		ClusterId:info.GetClusterID(),
		Ctime:info.GetCTime(),
	}
}

func BlockKeyProtoToBlockKey(bk *hdfs.BlockKeyProto) *BlockKey {
	return &BlockKey {
		Keyid: bk.GetKeyId(),
		ExpiryDate:bk.GetExpiryDate(),
		KeyBytes:bk.GetKeyBytes(),
	}
}

func ExportedBlockKeysProtoToExportedBlockKeys(ebk *hdfs.ExportedBlockKeysProto) ExportBlockKey {
	current := BlockKeyProtoToBlockKey(ebk.GetCurrentKey())
	allkeys := ebk.GetAllKeys()
	all := make([]*BlockKey, 0, len(allkeys))
	for _, key := range allkeys {
		all = append(all, BlockKeyProtoToBlockKey(key))
	}

	return ExportBlockKey {
		IsBlockTokenEnabled: ebk.GetIsBlockTokenEnabled(),
		KeyUpdateInterval: ebk.GetKeyUpdateInterval(),
		TokenLifeTime: ebk.GetTokenLifeTime(),
		CurrentKey: current,
		AllKeys: all,
	}
}

func convertRegisterProtoToDatanode(reg *hdsp.DatanodeRegistrationProto) *Datanode {
	return &Datanode {
		Id: DatanodeIDProtoToDatanodeID(reg.GetDatanodeID()),
		Info: StorageInfoProtoToStorageInfo(reg.GetStorageInfo()),
		Keys: ExportedBlockKeysProtoToExportedBlockKeys(reg.GetKeys()),
		SoftVersion: reg.GetSoftwareVersion(),
	}
}

func getIpAddrFromContext(ctx context.Context) string {
	reqInfo := logger.GetReqInfo(ctx)
	if reqInfo == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(reqInfo.RemoteHost)
	if err != nil {
		return ""
	}
	log.Printf("remoteHost %v, host %v", reqInfo.RemoteHost, host)
	return host
}

func opfsRegisterDatanode(ctx context.Context, r *hdsp.RegisterDatanodeRequestProto) (*hdsp.RegisterDatanodeResponseProto, error) {
	reg := r.GetRegistration()
	log.Printf("datanode id %v, storageinfo %v, Keys %v, version %v", reg.GetDatanodeID(), reg.GetStorageInfo(),
									reg.GetKeys(), reg.GetSoftwareVersion())
	datanode := convertRegisterProtoToDatanode(reg)
	ipAddr := getIpAddrFromContext(ctx)
	if ipAddr != "" {
		datanode.Id.Ipaddr = ipAddr
	}
	datamap := GetGlobalDatanodeMap()
	err := datamap.Register(datanode)
	if err != nil {
		return nil, err
	}
	return &hdsp.RegisterDatanodeResponseProto {
		Registration: reg,
	}, nil

}
