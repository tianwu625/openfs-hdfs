package datanode

import (
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
)

func opBlockChecksum(r *hdfs.OpBlockChecksumProto) (*hdfs.BlockOpResponseProto, error) {
	res := new(hdfs.BlockOpResponseProto)

	return res, nil
}
