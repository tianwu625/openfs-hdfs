package datanode

import (
	"context"
	"log"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func deleteBlockPoolDec(b []byte) (proto.Message, error) {
	req := new(hdfs.DeleteBlockPoolRequestProto)
	return parseRequest(b, req)
}

func deleteBlockPool(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.DeleteBlockPoolRequestProto)
	res, err := opfsDeleteBlockPool(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsDeleteBlockPool(r *hdfs.DeleteBlockPoolRequestProto)(*hdfs.DeleteBlockPoolResponseProto, error) {
	poolName := r.GetBlockPool()
	force := r.GetForce()
	log.Printf("name %v force %v", poolName, force)

	//for now Block Pool for openfs is filename
	//Block id is offset of filename
	//delete Block for openfs is delete file, but datanode should not delete file

	return new(hdfs.DeleteBlockPoolResponseProto), nil
}
