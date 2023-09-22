package cmd

import (
	"log"
	"os"
	"math"
	"context"
	"fmt"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/datanodeMap"
	"github.com/openfs/openfs-hdfs/internal/opfsBlocksMap"
)

func getBlockLocationsDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetBlockLocationsRequestProto)
	return parseRequest(b, req)
}

func getBlockLocations(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetBlockLocationsRequestProto)
	log.Printf("src %v\noffset %v\nLength %v\n", req.GetSrc(), req.GetOffset(), req.GetLength())
	return opfsGetBlockLocations(req)
}

const (
	defaultBlockSize = 128 * 1024 * 1024 // 128 MB
)

func getFileSize(src string) (int64, error) {
	f, err := opfs.Open(src)
	if err != nil {
		return 0, nil
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, nil
	}
	return fi.Size(), nil
}

func minInt64(nums ...int64) int64{
	var min int64 = math.MaxInt64
	for _, i := range nums {
		if i < min {
			min = i
		}
	}

	return min
}

func getStorageType(stype string) hdfs.StorageTypeProto {
	switch(stype) {
	case "DISK":
		return hdfs.StorageTypeProto_DISK
	}
	return hdfs.StorageTypeProto_DISK
}

func convertDatanodeIdProto(d *datanodeMap.Datanode) *hdfs.DatanodeIDProto {
	return &hdfs.DatanodeIDProto {
		IpAddr: proto.String(d.Id.Ipaddr),
		HostName:proto.String(d.Id.Hostname),
		DatanodeUuid: proto.String(d.Id.Uuid),
		XferPort: proto.Uint32(d.Id.Xferport),
		InfoPort: proto.Uint32(d.Id.Infoport),
		IpcPort: proto.Uint32(d.Id.Ipcport),
		InfoSecurePort: proto.Uint32(d.Id.Infosecureport),
	}
}

const (
	defaultRack = "/default-rack"
)

func getDatanodeLocs(b *opfsBlocksMap.Blockmap) ([]*hdfs.DatanodeInfoProto, []hdfs.StorageTypeProto, []string) {
	log.Printf("b %+v", b)
	locs := b.L
	infos := make([]*hdfs.DatanodeInfoProto, 0, len(locs))
	types := make([]hdfs.StorageTypeProto, 0, len(locs))
	sids := make([]string, 0, len(locs))
	dm := datanodeMap.GetGlobalDatanodeMap()
	for _, l := range locs {
		d := dm.GetDatanodeFromUuid(l.DatanodeUuid)
		if d == nil {
			continue
		}
		s := dm.GetStorageInfoFromUuid(l.DatanodeUuid, l.StorageUuid)
		if s == nil {
			continue
		}
		info := &hdfs.DatanodeInfoProto {
			Id: convertDatanodeIdProto(d),
			Location: proto.String(defaultRack),
		}
		stype := getStorageType(s.StorageType)
		infos = append(infos, info)
		types = append(types, stype)
		sids = append(sids, s.Uuid)
	}

	return infos, types, sids
}

func getDatanodeInfo(eb *hdfs.ExtendedBlockProto) []*hdfs.DatanodeInfoProto {
	datanodes := make([]*hdfs.DatanodeInfoProto, 0)
	id := new(hdfs.DatanodeIDProto)
	id.IpAddr = proto.String("0.0.0.0")
	hostname, _ := os.Hostname()
	id.HostName= proto.String(hostname)
	id.DatanodeUuid = proto.String(hostname + "-" + "127.0.0.1")
	id.XferPort = proto.Uint32(defaultDataNodeXferPort)
	id.InfoPort = proto.Uint32(50075)
	id.IpcPort = proto.Uint32(50020)
	id.InfoSecurePort = proto.Uint32(50475)
	datanode := new(hdfs.DatanodeInfoProto)
	datanode.Id = id

	datanodes = append(datanodes, datanode)

	return datanodes
}
/*
blocks fileLength:5333  blocks:{b:{poolId:"/x"  blockId:0  generationStamp:1000  numBytes:5333}  offset:0  locs:{id:{ipAddr:"0.0.0.0"  hostName:"ec-1"  datanodeUuid:"ec-1-127.0.0.1"  xferPort:50010  infoPort:50075  ipcPort:50020  infoSecurePort:50475}}  corrupt:false  blockToken:{identifier:""  password:""  kind:""  service:""}  isCached:false  storageTypes:DISK  storageIDs:"openfs-xx"}  underConstruction:false  lastBlock:{b:{poolId:"/x"  blockId:0  generationStamp:1000  numBytes:5333}  offset:0  locs:{id:{ipAddr:"0.0.0.0"  hostName:"ec-1"  datanodeUuid:"ec-1-127.0.0.1"  xferPort:50010  infoPort:50075  ipcPort:50020  infoSecurePort:50475}}  corrupt:false  blockToken:{identifier:""  password:""  kind:""  service:""}  isCached:false  storageTypes:DISK  storageIDs:"openfs-xx"}  isLastBlockComplete:true
*/
func opfsGetBlocks(src string, off uint64, length uint64) (*hdfs.LocatedBlocksProto, error) {
	size, err := getFileSize(src)
	if err != nil {
		return nil, err
	}

	if int64(off + length) > size {
		log.Printf("off %v, length %v, size %v, change to %v\n", off, length, size, uint64(size) - off)
		length = uint64(size) - off
	}

	bsm := getBlocksMap()

	bs, err := bsm.GetFileAllBlocks(src)
	if err != nil {
		if err := bsm.LoadFileAllBlocks(src); err != nil {
			return nil, err
		}
		bs, err = bsm.GetFileAllBlocks(src)
		if err != nil {
			return nil, err
		}
	}
	blocksProto := &hdfs.LocatedBlocksProto {
		FileLength: proto.Uint64(uint64(size)),
		UnderConstruction: proto.Bool(false),
		Blocks: make([]*hdfs.LocatedBlockProto, 0, len(bs)),
		IsLastBlockComplete: proto.Bool(true),
	}
	if len(bs) == 0 {
		if size != 0 {
			panic(fmt.Errorf("file %v size %v but blocks is %v", src, size, bs))
		}
		log.Printf("this file %v is zero size", src)
		return blocksProto, nil
	}
	lastb := bs[len(bs) - 1]
	start, _:= bsm.GetOffIndex(src, off)
	end, _:= bsm.GetOffIndex(src, off + length - 1)
	end += 1
	log.Printf("start %v, end %v", start, end)
	bs = bs[start:end]
	for i, b := range bs {
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
		blocksProto.Blocks = append(blocksProto.Blocks, lb)
		if i == len(bs) - 1{
			blocksProto.LastBlock = lb
			if b.OffStart == lastb.OffStart {
				blocksProto.IsLastBlockComplete = proto.Bool(true)
			} else {
				blocksProto.IsLastBlockComplete = proto.Bool(false)
			}
		}
	}

	return blocksProto, nil
}

func opfsGetBlockLocations(r *hdfs.GetBlockLocationsRequestProto)(*hdfs.GetBlockLocationsResponseProto, error) {
	 res := new(hdfs.GetBlockLocationsResponseProto)
	 src := r.GetSrc()
	 off := r.GetOffset()
	 length := r.GetLength()
	 blocksproto, err := opfsGetBlocks(src, off, length)
	 if err != nil {
		 return res, err
	 }
	 log.Printf("blocks %v", blocksproto)
	 res.Locations = blocksproto
	 return res, nil
}
