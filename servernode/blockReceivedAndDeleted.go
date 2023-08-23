package servernode

import (
	"log"
	"context"

	"google.golang.org/protobuf/proto"
	hdsp "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_server"
	"github.com/openfs/openfs-hdfs/internal/rpc"
)

func blockReceivedAndDeletedDec(b []byte) (proto.Message, error) {
	req := new(hdsp.BlockReceivedAndDeletedRequestProto)
	return rpc.ParseRequest(b, req)
}

func blockReceivedAndDeleted(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdsp.BlockReceivedAndDeletedRequestProto)
	res, err := opfsBlockReceivedAndDeleted(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsBlockReceivedAndDeleted(r *hdsp.BlockReceivedAndDeletedRequestProto)(*hdsp.BlockReceivedAndDeletedResponseProto, error) {
	reg := r.GetRegistration()
	poolId := r.GetBlockPoolId()
	blocks := r.GetBlocks()
	log.Printf("req %v", r)
	log.Printf("reg %v", reg)
	log.Printf("poolId %v", poolId)
	log.Printf("blocks %v", blocks)

	for _, b := range blocks {
		uuid := b.GetStorageUuid()
		bs := b.GetBlocks()
		s := b.GetStorage()
		log.Printf("uuid %v, bs %v, storage %v", uuid, bs, s)
		for _, nb := range bs {
			block := nb.GetBlock()
			status := nb.GetStatus().String()
			deleteHit := nb.GetDeleteHint()
			log.Printf("block %v, status %v, delete %v", block, status, deleteHit)
		}
	}

	return new(hdsp.BlockReceivedAndDeletedResponseProto), nil
}
