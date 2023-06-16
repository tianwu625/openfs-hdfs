package cmd

import (
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getServerDefaultsDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetServerDefaultsRequestProto)
	return parseRequest(b, req)
}

func getServerDefaults(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetServerDefaultsRequestProto)
	return opfsGetServerDefaults(req)
}

func opfsGetServerDefaults(r *hdfs.GetServerDefaultsRequestProto) (*hdfs.GetServerDefaultsResponseProto, error) {
	res := new(hdfs.GetServerDefaultsResponseProto)
	res.ServerDefaults = new(hdfs.FsServerDefaultsProto)
	res.ServerDefaults.BlockSize = proto.Uint64(134217728)
	res.ServerDefaults.BytesPerChecksum = proto.Uint32(512)
	res.ServerDefaults.WritePacketSize = proto.Uint32(65536)
	res.ServerDefaults.Replication = proto.Uint32(1)
	res.ServerDefaults.FileBufferSize = proto.Uint32(4096)
	res.ServerDefaults.EncryptDataTransfer = proto.Bool(false)
	res.ServerDefaults.TrashInterval = proto.Uint64(0)
	res.ServerDefaults.ChecksumType = hdfs.ChecksumTypeProto_CHECKSUM_CRC32C.Enum()
	return res, nil
}

