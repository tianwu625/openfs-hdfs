package cmd

import (
	"context"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func listReconfigurablePropertiesDec(b []byte) (proto.Message, error) {
	req := new(hdfs.ListReconfigurablePropertiesRequestProto)
	return parseRequest(b, req)
}

func listReconfiguableProperties(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.ListReconfigurablePropertiesRequestProto)
	return opfsListReconfigurableProperties(req)
}

func opfsListReconfigurableProperties(r *hdfs.ListReconfigurablePropertiesRequestProto) (*hdfs.ListReconfigurablePropertiesResponseProto, error) {
	grf := getGlobalReconfig()
	properties := grf.ListProperties()

	return &hdfs.ListReconfigurablePropertiesResponseProto {
		Name: properties,
	}, nil
}
