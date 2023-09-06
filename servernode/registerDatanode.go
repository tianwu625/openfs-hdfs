package servernode

import (
	"log"
	"context"
	"strings"
	"net"

	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/rpc"
	hdsp "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_server"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"github.com/openfs/openfs-hdfs/internal/logger"
	"github.com/openfs/openfs-hdfs/internal/datanodeMap"
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


func DatanodeIDProtoToDatanodeID(id *hdfs.DatanodeIDProto) datanodeMap.Datanodeid {
	return datanodeMap.Datanodeid {
		Ipaddr: id.GetIpAddr(),
		Hostname: id.GetHostName(),
		Uuid: id.GetDatanodeUuid(),
		Xferport: id.GetXferPort(),
		Infoport: id.GetInfoPort(),
		Ipcport: id.GetIpcPort(),
		Infosecureport: id.GetInfoSecurePort(),
	}
}

func StorageInfoProtoToStorageInfo(info *hdfs.StorageInfoProto) datanodeMap.StorageInfo {
	return datanodeMap.StorageInfo {
		LayoutVersion: info.GetLayoutVersion(),
		NamespaceId:info.GetNamespceID(),
		ClusterId:info.GetClusterID(),
		Ctime:info.GetCTime(),
	}
}

func BlockKeyProtoToBlockKey(bk *hdfs.BlockKeyProto) *datanodeMap.BlockKey {
	return &datanodeMap.BlockKey {
		Keyid: bk.GetKeyId(),
		ExpiryDate:bk.GetExpiryDate(),
		KeyBytes:bk.GetKeyBytes(),
	}
}

func ExportedBlockKeysProtoToExportedBlockKeys(ebk *hdfs.ExportedBlockKeysProto) datanodeMap.ExportBlockKey {
	current := BlockKeyProtoToBlockKey(ebk.GetCurrentKey())
	allkeys := ebk.GetAllKeys()
	all := make([]*datanodeMap.BlockKey, 0, len(allkeys))
	for _, key := range allkeys {
		all = append(all, BlockKeyProtoToBlockKey(key))
	}

	return datanodeMap.ExportBlockKey {
		IsBlockTokenEnabled: ebk.GetIsBlockTokenEnabled(),
		KeyUpdateInterval: ebk.GetKeyUpdateInterval(),
		TokenLifeTime: ebk.GetTokenLifeTime(),
		CurrentKey: current,
		AllKeys: all,
	}
}

func convertRegisterProtoToDatanode(reg *hdsp.DatanodeRegistrationProto) *datanodeMap.Datanode {
	return &datanodeMap.Datanode {
		Id: DatanodeIDProtoToDatanodeID(reg.GetDatanodeID()),
		Info: StorageInfoProtoToStorageInfo(reg.GetStorageInfo()),
		Keys: ExportedBlockKeysProtoToExportedBlockKeys(reg.GetKeys()),
		SoftVersion: reg.GetSoftwareVersion(),
	}
}

const (
	ipv4Loop = "127.0.0.1"
)

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
	if strings.Contains(host, ":") {
		if net.ParseIP(host).IsLoopback() {
			return ipv4Loop
		}
		log.Printf("ipv6 address, maybe hadoop not support")
		return ""
	}

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
	datamap := datanodeMap.GetGlobalDatanodeMap()
	err := datamap.Register(datanode)
	if err != nil {
		return nil, err
	}
	return &hdsp.RegisterDatanodeResponseProto {
		Registration: reg,
	}, nil

}
