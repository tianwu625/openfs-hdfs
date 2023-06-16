package datanode

import (
	"log"
	"net"
	"errors"
	"fmt"
	"hash/crc32"
	"encoding/binary"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"github.com/openfs/openfs-hdfs/internal/opfs"
	"google.golang.org/protobuf/proto"
)

var errRangOffset = errors.New("len is over the sizeof block")

func opReadBlock(r *hdfs.OpReadBlockProto) (*hdfs.BlockOpResponseProto, *dataTask, error) {
	res := new(hdfs.BlockOpResponseProto)
	header := r.GetHeader().GetBaseHeader()
	client := r.GetHeader().GetClientName()
	o := r.GetOffset()
	l := r.GetLen()
	checkSum := r.GetSendChecksums()
	cache := r.GetCachingStrategy()
	block := header.GetBlock()
	log.Printf("opReadBlock: block %v\nclient %v\noffset %v\nlen %v\ncheck %v\ncache %v\n",
			block, client, o, l, checkSum, cache)
	log.Printf("opReadBlock: poolid %v\nblock id %v\ngs %v\nnum %v\n", block.GetPoolId(), block.GetBlockId(),
			block.GetGenerationStamp(), block.GetNumBytes())
	t := dataTask {
		src: block.GetPoolId(),
		op: "read",
		off: int64(block.GetBlockId() * defaultBlockSize + o),
		size: int64(l),
	}

	if t.size > int64(block.GetNumBytes() - o) {
		return res, &t, errRangOffset
	}
	res.Status = hdfs.Status_SUCCESS.Enum()
	res.ReadOpChecksumInfo = &hdfs.ReadOpChecksumInfoProto {
		Checksum: &hdfs.ChecksumProto {
			Type: hdfs.ChecksumTypeProto_CHECKSUM_CRC32C.Enum(),
			BytesPerChecksum: proto.Uint32(defaultChunkSize),
		},
		ChunkOffset: proto.Uint64(0),
	}

	return res, &t, nil
}

const (
	defaultChunkSize = 512
)

//pack of data: | pktLen | headLen | pktHeaderProto | chunksum ... | chunk data ...|
//pktLen : 4 bytes + chunsum all length + chunk data all length, occupy 4 byte
//headLen: length of pktHeaderProto encode, occupy 2 byte

func splitPackets(b []byte, sum int, per int) ([][]byte, error) {
	packs := make([][]byte, 0)
	packCount := (sum + per - 1) / per
	for i := 0; i < packCount; i++ {
		p := make([]byte, 6)
		pktHeader := new(hdfs.PacketHeaderProto)
		pktHeader.OffsetInBlock = proto.Int64(int64(i * per))
		pktHeader.Seqno = proto.Int64(int64(i))
		pktHeader.LastPacketInBlock = proto.Bool(false)
		if i + 1 == packCount {
			pktHeader.DataLen = proto.Int32(int32(sum - i * per))
		} else {
			pktHeader.DataLen = proto.Int32(int32(per))
		}
		pktHeader.SyncBlock = proto.Bool(false)
		headerBytes, err := proto.Marshal(pktHeader)
		if err != nil {
			return packs, err
		}
		headerLen := len(headerBytes)
		binary.BigEndian.PutUint16(p[4:], uint16(headerLen))
		checksumCount := 0
		if i + 1 == packCount {
			left := sum - i * per
			checksumCount = (left + defaultChunkSize - 1) / defaultChunkSize
		} else {
			checksumCount = (per + defaultChunkSize - 1) / defaultChunkSize
		}
		pktLen := uint32(4 + 4 * checksumCount + sum)
		binary.BigEndian.PutUint32(p, pktLen)
		checksums := make([]byte, 4 * checksumCount)
		table := crc32.MakeTable(crc32.Castagnoli)
		for j := 0; j < checksumCount; j++ {
			var v uint32
			if j + 1 == checksumCount {
				v = crc32.Checksum(b[(j * defaultChunkSize):], table)
			} else {
				v = crc32.Checksum(b[(j * defaultChunkSize):((j + 1) * defaultChunkSize)], table)
			}
			log.Printf("pack NO.%v, chunk NO.%v checksum %v\n",i, j, v)
			binary.BigEndian.PutUint32(checksums[(j * 4):], v)
		}
		p = append(p[:6], headerBytes...)
		p = append(p, checksums...)
		p = append(p, b...)
		packs = append(packs, p)
	}

	return packs, nil
}

func sendLast(seqno int, conn net.Conn) {
	p := make([]byte, 6)
	pktHeader := new(hdfs.PacketHeaderProto)
	pktHeader.OffsetInBlock = proto.Int64(0)
	pktHeader.Seqno = proto.Int64(int64(seqno))
	pktHeader.LastPacketInBlock = proto.Bool(true)
	pktHeader.DataLen = proto.Int32(0)
	pktHeader.SyncBlock = proto.Bool(false)
	headerBytes, _ := proto.Marshal(pktHeader)
	headerLen := len(headerBytes)
	binary.BigEndian.PutUint16(p[4:], uint16(headerLen))
	binary.BigEndian.PutUint32(p, 4)
	p = append(p[:6], headerBytes...)

	_, err := conn.Write(p)
	if err != nil {
		log.Printf("send last message fail %v\n", err)
	}
}


const (
	defaultPacketSize = 65536 //64KB
)

func getDataFromFile(t *dataTask, conn net.Conn) error {
	f, err := opfs.Open(t.src)
	if err != nil {
		return err
	}
	defer f.Close()
	b := make([]byte, t.size)
	n, err := f.ReadAt(b, t.off)
	if err != nil || n != len(b) {
		return fmt.Errorf("fail to read %v, %v, %v", t.src, t.off, t.size)
	}
	packs, err := splitPackets(b, n, defaultPacketSize)
	for _, p := range packs {
		_, err = conn.Write(p)
		if err != nil {
			return err
		}
	}
	sendLast(len(packs), conn)

	return nil
}
