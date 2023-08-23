package cmd

import (
	"context"
	"log"
	"time"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"github.com/openfs/openfs-hdfs/servernode"
	"google.golang.org/protobuf/proto"
)

func addBlockDec(b []byte) (proto.Message, error) {
	req := new(hdfs.AddBlockRequestProto)
	return parseRequest(b, req)
}

func addBlock(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.AddBlockRequestProto)
	log.Printf("src %v\nclient %v\n", req.GetSrc(), req.GetClientName())
	log.Printf("Pre %v\nExcludeNodes %v\n", req.GetPrevious(), req.GetExcludeNodes())
	log.Printf("fileid %v\nfavorNodes %v\n", req.GetFileId(), req.GetFavoredNodes())
	log.Printf("Flags %v\n", req.GetFlags())
	return opfsAddBlock(req)
}

var (
	blockStart uint64
	defaultGenMode uint64 = 10000
	genStart uint64
)


func init() {
	blockStart = uint64(time.Now().Unix())
	defaultGenMode = 10000
	genStart = uint64(time.Now().Unix() % 10000)
}

func convertDatanodeInfoToProto(locs []*servernode.DatanodeLoc) []*hdfs.DatanodeInfoProto {
	res := make([]*hdfs.DatanodeInfoProto, 0, len(locs))
	for _, loc := range locs {
		node := loc.Node
		r := &hdfs.DatanodeInfoProto {
			Id: &hdfs.DatanodeIDProto {
				IpAddr: proto.String(node.Id.Ipaddr),
				HostName: proto.String(node.Id.Hostname),
				DatanodeUuid:proto.String(node.Id.Uuid),
				XferPort:proto.Uint32(node.Id.Xferport),
				InfoPort:proto.Uint32(node.Id.Infoport),
				IpcPort:proto.Uint32(node.Id.Ipcport),
				InfoSecurePort: proto.Uint32(node.Id.Infosecureport),
			},
		}
		res = append(res, r)
	}

	return res
}

func datanodeStorageToProto(locs []*servernode.DatanodeLoc) []string {
	res := make([]string, 0, len(locs))

	for _, loc := range locs {
		res = append(res, loc.Storage.Uuid)
	}

	return res
}

func opfsAddBlock(r *hdfs.AddBlockRequestProto) (*hdfs.AddBlockResponseProto, error) {
	res := new(hdfs.AddBlockResponseProto)
	src := r.GetSrc()
	size, err := getFileSize(src)
	if err != nil {
		return res, err
	}
	datanodes := servernode.GetGlobalDatanodeMap()
	options := servernode.AllocMethodOptions {
		Method: servernode.ReplicateMethod,
		Replicate: 1,
		StorageType: "DISK",
	}
	locs, err := datanodes.GetDatanodeWithAllocMethod(options)
	if err != nil {
		return nil, err
	}
	log.Printf("locs %v, datanodes %v", locs, datanodes)

	/*
	eb := new(hdfs.ExtendedBlockProto)
	eb.PoolId = proto.String(src)
	eb.BlockId = proto.Uint64(uint64(size/defaultBlockSize))
	eb.GenerationStamp = proto.Uint64(1000)
	eb.NumBytes = proto.Uint64(uint64(size%defaultBlockSize))
	*/
	eb := &hdfs.ExtendedBlockProto {
		PoolId: proto.String("/"),
		BlockId: proto.Uint64(blockStart),
		GenerationStamp: proto.Uint64(genStart),
		NumBytes:proto.Uint64(uint64(size%defaultBlockSize)),
	}
	lb := new(hdfs.LocatedBlockProto)
	lb.B = eb
	lb.Offset = proto.Uint64(0)
//	lb.Offset = proto.Uint64(uint64((size/defaultBlockSize) * defaultBlockSize))
//	lb.Locs = getDatanodeInfo(eb)
	lb.Locs = convertDatanodeInfoToProto(locs)
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
	lb.StorageIDs = datanodeStorageToProto(locs)
	log.Printf("ids %v", lb.StorageIDs)
	res.Block = lb
	log.Printf("res %v\n", res)

	return res, nil
}
