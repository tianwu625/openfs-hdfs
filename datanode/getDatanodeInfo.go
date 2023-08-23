package datanode

import (
	"context"
	"time"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getDatanodeInfoDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetDatanodeInfoRequestProto)
	return parseRequest(b, req)
}

func getDatanodeInfo(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetDatanodeInfoRequestProto)
	res, err := opfsGetDatanodeInfo(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsGetDatanodeInfo(r *hdfs.GetDatanodeInfoRequestProto) (*hdfs.GetDatanodeInfoResponseProto, error) {
	info := &hdfs.DatanodeLocalInfoProto {
		SoftwareVersion: proto.String(globalDatanodeSys.attrs.softwareVersion),
		ConfigVersion: proto.String("core-3.0.0,hdfs-1"),
		Uptime: proto.Uint64(uint64(time.Now().Sub(globalStartTime).Seconds())),
	}

	return &hdfs.GetDatanodeInfoResponseProto {
		LocalInfo: info,
	}, nil
}
