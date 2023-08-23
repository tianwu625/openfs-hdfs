package cmd

import (
	"context"
	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
)

func refreshCallQueueDec(b []byte) (proto.Message, error) {
	req := new(hadoop.RefreshCallQueueRequestProto)
	return parseRequest(b, req)
}

func refreshCallQueue(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hadoop.RefreshCallQueueRequestProto)
	return opfsRefreshCallQueue(req)
}

func opfsRefreshCallQueue(r *hadoop.RefreshCallQueueRequestProto) (*hadoop.RefreshCallQueueResponseProto, error) {
	return new(hadoop.RefreshCallQueueResponseProto), nil
}
