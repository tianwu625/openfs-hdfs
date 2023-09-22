package cmd

import (
	"context"
	"fmt"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getServerDefaultsDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetServerDefaultsRequestProto)
	return parseRequest(b, req)
}

func getServerDefaults(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetServerDefaultsRequestProto)
	return opfsGetServerDefaults(req)
}

func convertChecksumTypeToProto(checkType string) *hdfs.ChecksumTypeProto {
	switch checkType {
	case "NULL":
		return hdfs.ChecksumTypeProto_CHECKSUM_NULL.Enum()
	case "CRC32":
		return hdfs.ChecksumTypeProto_CHECKSUM_CRC32.Enum()
	case "CRC32C":
		return hdfs.ChecksumTypeProto_CHECKSUM_CRC32C.Enum()
	default:
		panic(fmt.Errorf("check type %s", checkType))
	}

	return hdfs.ChecksumTypeProto_CHECKSUM_CRC32C.Enum()
}

func opfsGetServerDefaults(r *hdfs.GetServerDefaultsRequestProto) (*hdfs.GetServerDefaultsResponseProto, error) {
	ccf := getGlobalConstConf()
	return &hdfs.GetServerDefaultsResponseProto {
		ServerDefaults: &hdfs.FsServerDefaultsProto {
			BlockSize: proto.Uint64(ccf.blockSize),
			BytesPerChecksum: proto.Uint32(ccf.bytesPerChecksum),
			WritePacketSize: proto.Uint32(ccf.writePacketSize),
			Replication: proto.Uint32(ccf.replicate),
			FileBufferSize: proto.Uint32(ccf.fileBufferSize),
			EncryptDataTransfer: proto.Bool(ccf.encryptDataTransfer),
			TrashInterval: proto.Uint64(ccf.trashInterval),
			ChecksumType: convertChecksumTypeToProto(ccf.crcChunkMethod),
		},
	}, nil
}

