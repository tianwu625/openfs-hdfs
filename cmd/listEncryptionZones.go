package cmd

import (
	"log"
	"context"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func listEncryptionZonesDec(b []byte) (proto.Message, error) {
	req := new(hdfs.ListEncryptionZonesRequestProto)
	return parseRequest(b, req)
}

func listEncryptionZones(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.ListEncryptionZonesRequestProto)
	log.Printf("id %v\n", req.GetId())
	return opfsListEncryptionZones(req)
}

func opfsListEncryptionZones(r *hdfs.ListEncryptionZonesRequestProto) (*hdfs.ListEncryptionZonesResponseProto, error) {
	return &hdfs.ListEncryptionZonesResponseProto {
		Zones: []*hdfs.EncryptionZoneProto{},
		HasMore: proto.Bool(false),
	}, nil
}
