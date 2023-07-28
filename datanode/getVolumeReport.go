package datanode

import (
	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getVolumeReportDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetVolumeReportRequestProto)
	return parseRequest(b, req)
}

func getVolumeReport(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetVolumeReportRequestProto)
	res, err := opfsGetVolume(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsGetVolume(r *hdfs.GetVolumeReportRequestProto) (*hdfs.GetVolumeReportResponseProto, error) {
	infos := make([]*hdfs.DatanodeVolumeInfoProto, 0, 1)

	fsinfo, err := opfs.StatFs()
	if err != nil {
		return nil, err
	}
	info := &hdfs.DatanodeVolumeInfoProto {
		Path: proto.String("/"),
		StorageType: hdfs.StorageTypeProto_DISK.Enum(),
		UsedSpace: proto.Uint64(fsinfo.Used),
		FreeSpace: proto.Uint64(fsinfo.Remaining),
		ReservedSpace: proto.Uint64(0),
		ReservedSpaceForReplicas: proto.Uint64(0),
		NumBlocks: proto.Uint64(0),
	}
	infos = append(infos, info)

	return &hdfs.GetVolumeReportResponseProto {
		VolumeInfo: infos,
	}, nil
}
