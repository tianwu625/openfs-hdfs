package cmd

import (
	"log"
	"fmt"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func restoreFailedStorageDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RestoreFailedStorageRequestProto)
	return parseRequest(b, req)
}

func restoreFailedStorage(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RestoreFailedStorageRequestProto)
	log.Printf("args %v\n", req.GetArg())
	return opfsRestoreFailedStorage(req)
}

func opfsRestoreFailedStorage(r *hdfs.RestoreFailedStorageRequestProto) (*hdfs.RestoreFailedStorageResponseProto, error) {
	args := r.GetArg()
	log.Printf("args %v\n",args)

	v := onValue
	switch args {
	case "true":
		if err := globalFs.SetRestoreFailedStorage(onValue); err != nil {
			return nil, err
		}
		v = globalFs.GetRestoreFailedStorage()
	case "false":
		if err := globalFs.SetRestoreFailedStorage(offValue); err != nil {
			return nil, err
		}
		v = globalFs.GetRestoreFailedStorage()
	case "check":
		v = globalFs.GetRestoreFailedStorage()
	default:
		panic(fmt.Errorf("not support args %v", args))
	}

	return &hdfs.RestoreFailedStorageResponseProto {
		Result: proto.Bool(v == onValue),
	},nil
}
