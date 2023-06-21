package cmd

import (
	"log"
	"os"
	"math"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
)

func getBlockLocationsDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetBlockLocationsRequestProto)
	return parseRequest(b, req)
}

func getBlockLocations(m proto.Message) (proto.Message, error) {
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

func getDatanodeInfo(eb *hdfs.ExtendedBlockProto) []*hdfs.DatanodeInfoProto {
	datanodes := make([]*hdfs.DatanodeInfoProto, 0)
	id := new(hdfs.DatanodeIDProto)
	id.IpAddr = proto.String("127.0.0.1")
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

func opfsGetBlocks(src string, off uint64, length uint64) (*hdfs.LocatedBlocksProto, error) {
	 size, err := getFileSize(src)
	 if err != nil {
		 return new(hdfs.LocatedBlocksProto), err
	 }
	 if int64(off + length) > size {
		 log.Printf("off %v, length %v, size %v, change to %v\n", off, length, size, uint64(size) - off)
		 length = uint64(size) - off
	 }
	 blocks := ((off + length) / defaultBlockSize - off / defaultBlockSize) + 1
	 first := off / defaultBlockSize
	 blocksproto := new(hdfs.LocatedBlocksProto)
	 blocksproto.FileLength = proto.Uint64(uint64(size))
	 blocksproto.UnderConstruction = proto.Bool(false)
	 blocksproto.Blocks = make([]*hdfs.LocatedBlockProto, 0)
	 for i:= uint64(0); i < blocks; i++ {
		eb := new(hdfs.ExtendedBlockProto)
		eb.PoolId = proto.String(src)
		eb.BlockId = proto.Uint64(first + uint64(i))
		eb.GenerationStamp = proto.Uint64(1000)
		eb.NumBytes = proto.Uint64(uint64(minInt64(int64(length - (first + uint64(i)) * defaultBlockSize), defaultBlockSize)))
		lb := new(hdfs.LocatedBlockProto)
		lb.B = eb
		lb.Offset = proto.Uint64((first + uint64(i)) * defaultBlockSize)
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
		blocksproto.Blocks = append(blocksproto.Blocks, lb)
		if i == blocks -1 {
			blocksproto.LastBlock = lb
			if lb.GetOffset() + defaultBlockSize >= blocksproto.GetFileLength() {
				blocksproto.IsLastBlockComplete = proto.Bool(true)
			} else {
				blocksproto.IsLastBlockComplete = proto.Bool(false)
			}
		}
	 }
	 return blocksproto, nil
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
