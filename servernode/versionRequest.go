package servernode

import (
	"os"
	"io"
	"strings"
	"strconv"
	"fmt"
	"context"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/rpc"
)

func versionRequestDec(b []byte) (proto.Message, error) {
	req := new(hdfs.VersionRequestProto)
	return rpc.ParseRequest(b, req)
}

func versionRequest(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.VersionRequestProto)
	res, err := opfsVersionRequest(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

/*
namespaceID=214074233
clusterID=CID-bb6b2ad0-b65d-4057-a081-16fa44ea5026
cTime=1689841792249
storageType=NAME_NODE
blockpoolID=BP-2012497695-192.168.21.164-1689841792249
layoutVersion=-66
 */
type fakeInfo struct {
	namespaceid uint32
	clusterid string
	ctime uint64
	blockpool string
	layout uint32
}

func getFakeInfo() *fakeInfo {
	res := new(fakeInfo)
	f, err := os.Open("/tmp/hadoop-root/dfs/name/current/VERSION")
	if err != nil {
		return nil
	}
	b, err := io.ReadAll(f)
	if err != nil {
		return nil
	}
	ss := strings.Split(string(b), "\n")
	for _, s := range ss {
		kv := strings.SplitN(s, "=", 2)
		switch kv[0] {
		case "namespaceID":
			tmp, _ := strconv.Atoi(kv[1])
			res.namespaceid = uint32(tmp)
		case "clusterID":
			res.clusterid = kv[1]
		case "cTime":
			res.ctime, _ = strconv.ParseUint(kv[1], 10, 64)
		case "blockpoolID":
			res.blockpool = kv[1]
		case "layoutVersion":
			tmp, _ := strconv.Atoi(kv[1])
			res.layout = uint32(tmp)
		default:
			fmt.Printf("k %v\n", kv[0])
		}
	}

	fmt.Printf("res %#v\n", *res)

	return res
}

func opfsVersionRequest(r *hdfs.VersionRequestProto) (*hdfs.VersionResponseProto, error) {
	fsinfo, err := opfs.StatFs()
	if err != nil {
		return nil, err
	}
	info := getFakeInfo()
	if info == nil {
		panic(fmt.Errorf("get fake info fail"))
	}
	return &hdfs.VersionResponseProto {
		Info: &hdfs.NamespaceInfoProto {
			BuildVersion: proto.String("7824c153d33bb0395652cfd20d57f7fabdf409ea"),
			Unused: proto.Uint32(uint32(fsinfo.Remaining)),
			BlockPoolID: proto.String("/"),
			StorageInfo: &hdfs.StorageInfoProto {
				LayoutVersion: proto.Uint32(info.layout),
				NamespceID: proto.Uint32(info.namespaceid),
				ClusterID: proto.String(info.clusterid),
				CTime: proto.Uint64(info.ctime),
			},
			SoftwareVersion: proto.String("3.3.5"),
			Capabilities: proto.Uint64(1),
			State: hdfs.NNHAStatusHeartbeatProto_ACTIVE.Enum(),
		},
	}, nil
}
