package servernode

import (
	"log"
	"context"
	"fmt"

	"google.golang.org/protobuf/proto"
	hdsp "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_server"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"github.com/openfs/openfs-hdfs/internal/rpc"
	"github.com/openfs/openfs-hdfs/internal/opfsBlocksMap"
	"github.com/openfs/openfs-hdfs/internal/logger"
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

func convertProtoToBlockMap(poolId, nodeuuid, storageuuid string, nb *hdfs.BlockProto) *opfsBlocksMap.Blockmap {
	return &opfsBlocksMap.Blockmap {
		B: &opfsBlocksMap.Block {
			PoolId: poolId,
			BlockId: nb.GetBlockId(),
			Generation: nb.GetGenStamp(),
			Num: nb.GetNumBytes(),
		},
		L: []*opfsBlocksMap.BlockLoc {
			&opfsBlocksMap.BlockLoc {
				DatanodeUuid: nodeuuid,
				StorageUuid: storageuuid,
			},
		},
	}
}

func opfsBlockReceivedAndDeleted(r *hdsp.BlockReceivedAndDeletedRequestProto)(*hdsp.BlockReceivedAndDeletedResponseProto, error) {
	reg := r.GetRegistration()
	datanodeuuid := reg.GetDatanodeID().GetDatanodeUuid()
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
			switch status {
			case "RECEIVING":
				fallthrough
			case "RECEIVED":
				obm := opfsBlocksMap.GetOpfsBlocksMap()
				b := convertProtoToBlockMap(poolId, datanodeuuid, uuid, block)
				if err := obm.CommitBlock(poolId, b); err != nil {
					logger.LogIf(nil, fmt.Errorf("commit fail %v", err))
					return nil, err
				}
			case "DELETED":
			default:
				panic(fmt.Errorf("status not support %v", status))
			}
			deleteHit := nb.GetDeleteHint()
			log.Printf("block %v, status %v, delete %v", block, status, deleteHit)
		}
	}

	return new(hdsp.BlockReceivedAndDeletedResponseProto), nil
}
