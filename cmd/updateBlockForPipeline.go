package cmd

import (
	"log"
	"context"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
)

func updateBlockForPipelineDec(b []byte) (proto.Message, error) {
	req := new(hdfs.UpdateBlockForPipelineRequestProto)
	return parseRequest(b, req)
}

func updateBlockForPipeline(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.UpdateBlockForPipelineRequestProto)
	log.Printf("block %v\nclient %v\n", req.GetBlock(), req.GetClientName())
	return opfsUpdateBlockForPipeline(req)
}

func opfsUpdateBlockForPipeline(r *hdfs.UpdateBlockForPipelineRequestProto) (*hdfs.UpdateBlockForPipelineResponseProto, error) {
	res := new(hdfs.UpdateBlockForPipelineResponseProto)
	eb := r.GetBlock()
	lb := new(hdfs.LocatedBlockProto)
	lb.B = eb
	lb.Offset = proto.Uint64(uint64(eb.GetBlockId() * defaultBlockSize))
	lb.Locs = getDatanodeInfo(eb)
	lb.Corrupt = proto.Bool(false)
	lb.BlockToken = new(hadoop.TokenProto)
	lb.BlockToken.Identifier = []byte{}
	lb.BlockToken.Password = []byte{}
	lb.BlockToken.Kind = proto.String("")
	lb.BlockToken.Service = proto.String("")
	lb.IsCached = []bool {
		false,
	}
	lb.StorageTypes = []hdfs.StorageTypeProto {
		hdfs.StorageTypeProto_DISK,
	}
	lb.StorageIDs = []string {
		"openfs-xx",
	}
	res.Block = lb

	return res, nil
}
