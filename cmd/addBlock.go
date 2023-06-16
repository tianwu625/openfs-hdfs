package cmd

import (
	"log"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
)

func addBlockDec(b []byte) (proto.Message, error) {
	req := new(hdfs.AddBlockRequestProto)
	return parseRequest(b, req)
}

func addBlock(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.AddBlockRequestProto)
	log.Printf("src %v\nclient %v\n", req.GetSrc(), req.GetClientName())
	log.Printf("Pre %v\nExcludeNodes %v\n", req.GetPrevious(), req.GetExcludeNodes())
	log.Printf("fileid %v\nfavorNodes %v\n", req.GetFileId(), req.GetFavoredNodes())
	log.Printf("Flags %v\n", req.GetFlags())
	return opfsAddBlock(req)
}

func opfsAddBlock(r *hdfs.AddBlockRequestProto) (*hdfs.AddBlockResponseProto, error) {
	res := new(hdfs.AddBlockResponseProto)
	src := r.GetSrc()
	size, err := getFileSize(src)
	if err != nil {
		return res, err
	}

	eb := new(hdfs.ExtendedBlockProto)
	eb.PoolId = proto.String(src)
	eb.BlockId = proto.Uint64(uint64(size/defaultBlockSize))
	eb.GenerationStamp = proto.Uint64(1000)
	eb.NumBytes = proto.Uint64(uint64(size%defaultBlockSize))
	lb := new(hdfs.LocatedBlockProto)
	lb.B = eb
	lb.Offset = proto.Uint64(uint64((size/defaultBlockSize) * defaultBlockSize))
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
	log.Printf("res %v\n", res)

	return res, nil
}
