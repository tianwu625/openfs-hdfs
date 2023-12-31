package cmd

import (
	"context"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func startReconfigurationDec(b []byte) (proto.Message, error) {
	req := new(hdfs.StartReconfigurationRequestProto)
	return parseRequest(b, req)
}

func startReconfiguration(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.StartReconfigurationRequestProto)
	return opfsStartReconfiguration(req)
}

func opfsStartReconfiguration(r *hdfs.StartReconfigurationRequestProto) (*hdfs.StartReconfigurationResponseProto, error) {
	core, err := globalConfEnv.ReloadCore()
	if err != nil {
		return nil, err
	}
	conf, err := core.ParseReconfigNamenode()
	if err != nil {
		return nil, err
	}
	grf := getGlobalReconfig()
	err = grf.StartUpdateConf(conf)
	if err != nil {
		return nil, err
	}
	return new(hdfs.StartReconfigurationResponseProto), nil
}
