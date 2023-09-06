package cmd

import (
	"context"
	"log"
	"time"
	"sync/atomic"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"github.com/openfs/openfs-hdfs/internal/datanodeMap"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/opfsBlocksMap"
)

func addBlockDec(b []byte) (proto.Message, error) {
	req := new(hdfs.AddBlockRequestProto)
	return parseRequest(b, req)
}

func addBlock(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.AddBlockRequestProto)
	log.Printf("---------------------------------------------------")
	log.Printf("src %v\nclient %v\n", req.GetSrc(), req.GetClientName())
	log.Printf("!!!!!!!!!!!!!Pre %v\nExcludeNodes %v\n", req.GetPrevious(), req.GetExcludeNodes())
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

func convertDatanodeInfoToProto(locs []*datanodeMap.DatanodeLoc) []*hdfs.DatanodeInfoProto {
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

func datanodeStorageToProto(locs []*datanodeMap.DatanodeLoc) []string {
	res := make([]string, 0, len(locs))

	for _, loc := range locs {
		res = append(res, loc.Storage.Uuid)
	}

	return res
}

func getGen() uint64 {
	return atomic.AddUint64(&genStart, 1)
}

func convertProtoToBlockMap(b *hdfs.ExtendedBlockProto) *opfsBlocksMap.Blockmap {
	if b == nil {
		return nil
	}
	return &opfsBlocksMap.Blockmap {
		B: &opfsBlocksMap.Block {
			PoolId: b.GetPoolId(),
			BlockId: b.GetBlockId(),
			Generation: b.GetGenerationStamp(),
			Num: b.GetNumBytes(),
		},
	}
}

func opfsAddBlock(r *hdfs.AddBlockRequestProto) (*hdfs.AddBlockResponseProto, error) {
	res := new(hdfs.AddBlockResponseProto)
	src := r.GetSrc()
	excludeNodes := r.GetExcludeNodes()
	excludes := make([]string, 0, len(excludeNodes))
	for _, node := range excludeNodes {
		excludes = append(excludes, node.GetId().GetDatanodeUuid())
	}
	prev := r.GetPrevious()
	bprev := convertProtoToBlockMap(prev)
	bsm := getBlocksMap()
	b, err := bsm.AppendBlock(src, bprev, excludes)
	if err != nil {
		return nil, err
	}
	eb := &hdfs.ExtendedBlockProto {
		PoolId:proto.String(b.B.PoolId),
		BlockId: proto.Uint64(b.B.BlockId),
		GenerationStamp: proto.Uint64(b.B.Generation),
		NumBytes: proto.Uint64(b.B.Num),
	}
	lb := &hdfs.LocatedBlockProto {
		B: eb,
		Offset: proto.Uint64(b.OffStart),
		Corrupt: proto.Bool(false),
		BlockToken: &hadoop.TokenProto{
			Identifier: []byte{},
			Password: []byte{},
			Kind: proto.String(""),
			Service: proto.String(""),
		},
	}
	lb.Locs, lb.StorageTypes, lb.StorageIDs = getDatanodeLocs(b)
	lb.IsCached = []bool {
		false,
	}
	res.Block = lb
	log.Printf("res %v\n", res)

	return res, nil
}
