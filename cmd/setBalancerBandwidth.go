package cmd

import (
	"log"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func setBalancerBandwidthDec(b []byte) (proto.Message, error) {
	req := new(hdfs.SetBalancerBandwidthRequestProto)
	return parseRequest(b, req)
}

func setBalancerBandwidth(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.SetBalancerBandwidthRequestProto)
	log.Printf("bandwidth", req.GetBandwidth())
	res, err := opfsSetBalancerBandwidth(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsSetBalancerBandwidth(r *hdfs.SetBalancerBandwidthRequestProto) (*hdfs.SetBalancerBandwidthResponseProto, error) {
	bandwidth := r.GetBandwidth()

	log.Printf("newbandwidth %v", bandwidth)
	//need to update bandwidth to update datanode bandwidth

	return new(hdfs.SetBalancerBandwidthResponseProto), nil
}
