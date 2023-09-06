package datanode

import (
	"log"
	"net"
	"fmt"
	"encoding/binary"
	"strings"
	"path"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"github.com/openfs/openfs-hdfs/internal/opfs"
	"google.golang.org/protobuf/proto"
)

func opReadBlock(r *hdfs.OpReadBlockProto) (*hdfs.BlockOpResponseProto, *dataTask, error) {
	res := new(hdfs.BlockOpResponseProto)
	header := r.GetHeader().GetBaseHeader()
	client := r.GetHeader().GetClientName()
	o := r.GetOffset()
	if o > defaultChunkSize {
		panic(fmt.Errorf("offset %v is not off in block but off in file", o))
	}
	l := r.GetLen()
	checkSum := r.GetSendChecksums()
	cache := r.GetCachingStrategy()
	block := header.GetBlock()
	log.Printf("opReadBlock: block %v\nclient %v\noffset %v\nlen %v\ncheck %v\ncache %v\n",
			block, client, o, l, checkSum, cache)
	log.Printf("opReadBlock: poolid %v\nblock id %v\ngs %v\nnum %v\n", block.GetPoolId(), block.GetBlockId(),
			block.GetGenerationStamp(), block.GetNumBytes())
	src :=  block.GetPoolId()
	off := int64(block.GetBlockId() * defaultBlockSize + o - o%defaultChunkSize)
	if !strings.Contains(src, "/") || src == "/" {
		off = 0
		src = path.Join("/", block.GetPoolId(), fmt.Sprintf("%d", block.GetBlockId()))
	}
	t := dataTask {
		src: src,
		op: "read",
		off: off,
	}
	t.packstart = int64(o - o % defaultChunkSize)
	t.size = int64((l + defaultChunkSize - 1 + o - o%defaultChunkSize)/defaultChunkSize * defaultChunkSize)

	if t.size > int64(block.GetNumBytes() - (o - o%defaultChunkSize)) {
		t.size = int64(block.GetNumBytes() - (o - o%defaultChunkSize))
	}

	res.Status = hdfs.Status_SUCCESS.Enum()
	res.ReadOpChecksumInfo = &hdfs.ReadOpChecksumInfoProto {
		Checksum: &hdfs.ChecksumProto {
			Type: hdfs.ChecksumTypeProto_CHECKSUM_CRC32C.Enum(),
			BytesPerChecksum: proto.Uint32(defaultChunkSize),
		},
		ChunkOffset: proto.Uint64(o - o%defaultChunkSize),
	}

	return res, &t, nil
}

type packetRequest struct {
	offset int
	seqno int
	lastInBlock bool
	syncblock bool
	checksums []byte
	data []byte
}

func writePacket(r *packetRequest, conn net.Conn) error {
	headerInfo := &hdfs.PacketHeaderProto {
		OffsetInBlock: proto.Int64(int64(r.offset)),
		Seqno: proto.Int64(int64(r.seqno)),
		LastPacketInBlock: proto.Bool(r.lastInBlock),
		DataLen: proto.Int32(int32(len(r.data))),
		SyncBlock: proto.Bool(r.syncblock),
	}

        totalLength := len(r.data) + len(r.checksums) + 4

        header := make([]byte, 6, 6+totalLength)
        infoBytes, err := proto.Marshal(headerInfo)
        if err != nil {
                return err
        }

        binary.BigEndian.PutUint32(header, uint32(totalLength))
        binary.BigEndian.PutUint16(header[4:], uint16(len(infoBytes)))
        header = append(header, infoBytes...)
        header = append(header, r.checksums...)
        header = append(header, r.data...)

        _, err = conn.Write(header)
        if err != nil {
                return err
        }

        return nil
}

const (
	defaultChunkSize = 512
)

//pack of data: | pktLen | headLen | pktHeaderProto | chunksum ... | chunk data ...|
//pktLen : 4 bytes + chunsum all length + chunk data all length, occupy 4 byte
//headLen: length of pktHeaderProto encode, occupy 2 byte

func splitPacketsAndSent(b []byte, sum int, per int, start int64, conn net.Conn) (int, error) {
	packCount := (sum + per - 1) / per
	log.Printf("packCount %v, per %v, start %v, sum %v, len %v", packCount, per, start, sum, len(b))
	for i := 0; i < packCount; i++ {
		pdata := []byte{}
		poff := i * per
		if i + 1 == packCount {
			pdata = b[poff:]
		} else {
			pdata = b[poff:poff+per]
		}
		checksumArgs := &argsFileInfo {
			size: int64(len(pdata)),
			perSize: defaultChunkSize,
			checktype: "CHECKSUM_CRC32C",
			bdata: pdata,
		}
		checksums, err := getCrcBytes(checksumArgs)
		if err != nil {
			return i, err
		}
		req := &packetRequest {
			offset: i * per + int(start),
			seqno : i,
			lastInBlock:false,
			syncblock: false,
			checksums:checksums,
			data:pdata,
		}
		if err := writePacket(req, conn); err != nil{
			return i, err
		}
	}

	return packCount, nil
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

	log.Printf("send last message !!!!")
	_, err := conn.Write(p)
	if err != nil {
		log.Printf("send last message fail %v\n", err)
	}
}


const (
	defaultPacketSize = 65536 //64KB
)

func opfsGetData(src string, off int64, b []byte) (int, error) {
	f, err := opfs.Open(src)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	log.Printf("off %v, len %v\n", off, len(b))
	n, err := f.ReadAt(b, off)
	if err != nil {
		return 0, err
	}

	return n, nil
}

func getDataFromFile(t *dataTask, conn net.Conn) error {
	b := make([]byte, t.size)
	n, err := opfsGetData(t.src, t.off, b)
	if err != nil || n != len(b) {
		return fmt.Errorf("fail to read %v, %v, %v", t.src, t.off, t.size)
	}
	seq, err := splitPacketsAndSent(b, n, defaultPacketSize, t.packstart, conn)
	if err != nil {
		return err
	}
	sendLast(seq, conn)

	return nil
}
