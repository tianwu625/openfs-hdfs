package datanode

import (
	"log"
	"net"
	"math"
	"io"
	"errors"
	"fmt"
	"encoding/binary"
	"bytes"
	"hash/crc32"
	"os"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"github.com/openfs/openfs-hdfs/internal/opfs"
	xfer "github.com/openfs/openfs-hdfs/internal/transfer"
	"google.golang.org/protobuf/proto"
)

func opWriteBlock(r *hdfs.OpWriteBlockProto) (*hdfs.BlockOpResponseProto, *dataTask, error) {
	res := new(hdfs.BlockOpResponseProto)
	header := r.GetHeader().GetBaseHeader()
	client := r.GetHeader().GetClientName()
	targets := r.GetTargets()
	source := r.GetSource()
	stage := r.GetStage()
	pipe := r.GetPipelineSize()
	minrecv := r.GetMinBytesRcvd()
	maxrecv := r.GetMaxBytesRcvd()
	laststamp := r.GetLatestGenerationStamp()
	checksum := r.GetRequestedChecksum()
	block := header.GetBlock()
	log.Printf("opWriteBlock: block %v\nclient %v\ntarget %v\nsource %v\nstage %v\npipe %v\n",
			block, client, targets, source, stage, pipe)
	log.Printf("min %v\nmax %v\nlaststamp %v\nchecksum %v\n",
		        minrecv, maxrecv, laststamp, checksum)
	log.Printf("opWriteBlock: poolid %v\nblock id %v\ngs %v\nnum %v\n", block.GetPoolId(), block.GetBlockId(),
			block.GetGenerationStamp(), block.GetNumBytes())
	t := dataTask {
		src: block.GetPoolId(),
		op: "write",
		off: int64(block.GetBlockId() * defaultBlockSize),
		size: int64(defaultBlockSize),
		checkMethod: checksum.GetType().String(),
		bytesPerCheck: checksum.GetBytesPerChecksum(),
	}

	/*
	if pipe != 0 {
		res.Status =hdfs.Status_ERROR_UNSUPPORTED.Enum()
		res.FirstBadLink = proto.String("")
		return res, &t, fmt.Errorf("pip is not 0")
	}
	*/
	res.Status = hdfs.Status_SUCCESS.Enum()
	res.FirstBadLink = proto.String("")

	return res, &t, nil
}

func readPacketHeader(conn net.Conn) (*hdfs.PacketHeaderProto, error) {
	lengthBytes := make([]byte, 6)
	n, err := io.ReadFull(conn, lengthBytes)
	if err != nil {
		fmt.Printf("read 6 byte failed %v %v\n", err, n)
		return nil, err
	}

	fmt.Printf("head rpc %v\n", binary.BigEndian.Uint32(lengthBytes[:4]))

	// We don't actually care about the total length.
	packetHeaderLength := binary.BigEndian.Uint16(lengthBytes[4:])
	fmt.Printf("length %v\n", packetHeaderLength)
	packetHeaderBytes := make([]byte, packetHeaderLength)
	_, err = io.ReadFull(conn, packetHeaderBytes)
	if err != nil {
		return nil, err
	}

	packetHeader := &hdfs.PacketHeaderProto{}
	err = proto.Unmarshal(packetHeaderBytes, packetHeader)

	return packetHeader, nil
}

var errInvalidChecksum error = errors.New("mismatch checksum")

func validateChecksum(b []byte, checksums bytes.Buffer, checktype string, index int) error {
	checksumOffset := 4 * index
	checksumBytes := checksums.Bytes()[checksumOffset : checksumOffset+4]
	checksum := binary.BigEndian.Uint32(checksumBytes)
	var checksumTab *crc32.Table
	switch checktype {
	case "CHECKSUM_CRC32":
		checksumTab = crc32.MakeTable(crc32.IEEE)
	case "CHECKSUM_CRC32C":
		checksumTab = crc32.MakeTable(crc32.Castagnoli)
	}
	crc := crc32.Checksum(b, checksumTab)
	fmt.Printf("len b %v, crc %v\n", len(b), crc)
	if crc != checksum {
		return errInvalidChecksum
	}

	return nil
}

func getPackData(t *dataTask, conn net.Conn) ([]byte, int64, int64, bool, error) {
	header, err := readPacketHeader(conn)
	if err != nil {
		return []byte{}, 0, 0, false, err
	}

	dataLength := int(header.GetDataLen())
	if dataLength == 0 {
		//should be a heartbeat
		return []byte{}, 0, header.GetSeqno(), header.GetLastPacketInBlock(), nil
	}
	numChunks := 0
	chunkSize := t.bytesPerCheck
	checksums := bytes.Buffer{}
	if t.checkMethod != "CHECKSUM_NULL" {
		numChunks = int(math.Ceil(float64(dataLength) / float64(chunkSize)))
		// TODO don't assume checksum size is 4
		checksumsLength := numChunks * 4
		checksums.Reset()
		checksums.Grow(checksumsLength)
		_, err = io.CopyN(&checksums, conn, int64(checksumsLength))
		if err != nil {
			return []byte{}, 0, 0, false, err
		}
	}
	datab := make([]byte, dataLength)
	_, err = io.ReadFull(conn, datab)
	if err != nil {
		return []byte{}, 0, 0, false, err
	}
	if t.checkMethod != "CHECKSUM_NULL" {
		for i := 0; i < numChunks; i++ {
			chunkOff := i * int(chunkSize)
			chunkEnd := chunkOff + int(chunkSize)
			if chunkEnd >= dataLength {
				chunkEnd = dataLength
			}
			err := validateChecksum(datab[chunkOff:chunkEnd], checksums, t.checkMethod, i)
			if err != nil {
				return []byte{}, 0, 0, false, nil
			}
		}
	}

	return datab, header.GetOffsetInBlock(), header.GetSeqno(), header.GetLastPacketInBlock(), nil
}

func makeWriteReply(seqno int64, err error) *hdfs.PipelineAckProto {
	ack := new(hdfs.PipelineAckProto)
	ack.Seqno = proto.Int64(seqno)
	status := hdfs.Status_SUCCESS
	if err != nil {
		status = hdfs.Status_ERROR
	}
	replys := make([]hdfs.Status, 0)
	replys = append(replys, status)
	ack.Reply = replys
	return ack
}

func sentReply(seqno int64, err error, conn net.Conn) {
	ack := makeWriteReply(seqno, err)
	b, err := xfer.MakePrefixedMessage(ack)
	if err != nil {
		panic(err)
	}
	_, err = conn.Write(b)
	if err != nil {
		log.Printf("sent reply failed %v\n", err)
	}
}


func putDataToFile(t *dataTask, conn net.Conn) error {
	f, err := opfs.OpenWithCreate(t.src, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	for {
		log.Printf("in write data\n")
		b, off, seqno, last, err := getPackData(t, conn)
		if err != nil {
			log.Printf("get data fail err %v\n", err)
			if errors.Is(err, io.EOF) {
				break
			} else {
				sentReply(seqno, err, conn)
				continue
			}
		}
		log.Printf("len %v off %v seqno %v last %v err %v\n",
			len(b), off, seqno, last, err)
		if len(b) != 0 {
			_, err := f.WriteAt(b, off + t.off)
			if err != nil {
				log.Printf("write fail %v\n", err)
				sentReply(seqno, err, conn)
				continue
			}
		}
		sentReply(seqno, err, conn)
		if last {
			break
		}
	}

	return nil
}
