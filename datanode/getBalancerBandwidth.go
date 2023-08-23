package datanode

import (
	"context"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getBalancerBandwidthDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetBalancerBandwidthRequestProto)
	return parseRequest(b, req)
}

func getBalancerBandwidth(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetBalancerBandwidthRequestProto)
	res, err := opfsGetBalancerBandwidth(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsGetBalancerBandwidth(r *hdfs.GetBalancerBandwidthRequestProto) (*hdfs.GetBalancerBandwidthResponseProto, error) {
	bandwidth := globalDatanodeSys.GetBandwidth()
	return &hdfs.GetBalancerBandwidthResponseProto {
		Bandwidth: proto.Uint64(bandwidth),
	}, nil
}
