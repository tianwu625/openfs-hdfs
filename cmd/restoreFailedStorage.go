package cmd

import (
	"log"
	"fmt"
	"context"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/fsmeta"
)

func restoreFailedStorageDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RestoreFailedStorageRequestProto)
	return parseRequest(b, req)
}

func restoreFailedStorage(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RestoreFailedStorageRequestProto)
	log.Printf("args %v\n", req.GetArg())
	return opfsRestoreFailedStorage(req)
}

func opfsRestoreFailedStorage(r *hdfs.RestoreFailedStorageRequestProto) (*hdfs.RestoreFailedStorageResponseProto, error) {
	args := r.GetArg()
	log.Printf("args %v\n",args)

	v := fsmeta.OnValue
	onValue := fsmeta.OnValue
	offValue := fsmeta.OffValue
	gfs := fsmeta.GetGlobalFsMeta()
	switch args {
	case "true":
		if err := gfs.SetRestoreFailedStorage(onValue); err != nil {
			return nil, err
		}
		v = gfs.GetRestoreFailedStorage()
	case "false":
		if err := gfs.SetRestoreFailedStorage(offValue); err != nil {
			return nil, err
		}
		v = gfs.GetRestoreFailedStorage()
	case "check":
		v = gfs.GetRestoreFailedStorage()
	default:
		panic(fmt.Errorf("not support args %v", args))
	}

	return &hdfs.RestoreFailedStorageResponseProto {
		Result: proto.Bool(v == onValue),
	},nil
}
