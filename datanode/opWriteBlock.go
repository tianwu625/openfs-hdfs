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
	"strings"
	"path"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	hdsp "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_server"
	"github.com/openfs/openfs-hdfs/internal/opfs"
	xfer "github.com/openfs/openfs-hdfs/internal/transfer"
	"google.golang.org/protobuf/proto"
)

/*
var (
	testblocks = 0
	testpackages = 0
)
*/

func opWriteBlock(r *hdfs.OpWriteBlockProto) (*hdfs.BlockOpResponseProto, *dataTask, error) {
	log.Printf("opWriteBlock req %v", r)
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
	blockSize := globalDatanodeSys.constConf.blockSize
	src := block.GetPoolId()
	off := int64(block.GetBlockId() * blockSize)
	if !strings.Contains(src, "/") || src == "/" {
		off = 0
		src = path.Join("/", block.GetPoolId(), fmt.Sprintf("%d", block.GetBlockId()))
	}
	t := dataTask {
		block: block,
		storageId: r.GetStorageId(),
		src: src,
		op: "write",
		off: off,
		size: int64(blockSize),
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
	//testblocks++
	res.Status = hdfs.Status_SUCCESS.Enum()
	res.FirstBadLink = proto.String("")

	return res, &t, nil
	/*
	res.Status =hdfs.Status_ERROR_UNSUPPORTED.Enum()
	res.FirstBadLink = proto.String("")
	return res, &t, fmt.Errorf("pip is not 0")
	*/
}

func readPacketHeader(conn net.Conn) (*hdfs.PacketHeaderProto, error) {
	lengthBytes := make([]byte, 6)
	n, err := io.ReadFull(conn, lengthBytes)
	if err != nil {
		fmt.Printf("read 6 byte failed %v %v\n", err, n)
		return nil, err
	}

//	fmt.Printf("head rpc %v\n", binary.BigEndian.Uint32(lengthBytes[:4]))

	// We don't actually care about the total length.
	packetHeaderLength := binary.BigEndian.Uint16(lengthBytes[4:])
//	fmt.Printf("length %v\n", packetHeaderLength)
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
	//fmt.Printf("len b %v, crc %v\n", len(b), crc)
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
/*
req registration:{datanodeID:{ipAddr:"0.0.0.0" hostName:"ec-1" datanodeUuid:"9b612313-b002-436a-9261-174a7045c8b1" xferPort:9866 infoPort:9864 ipcPort:9867 infoSecurePort:0} storageInfo:{layoutVersion:4294967239 namespceID:1349015764 clusterID:"CID-77a6faeb-66f5-4743-8370-807e2413d3c1" cTime:1690428558063} keys:{isBlockTokenEnabled:false keyUpdateInterval:0 tokenLifeTime:0 currentKey:{keyId:0 expiryDate:0 keyBytes:""}} softwareVersion:"3.3.5"} blockPoolId:"/" blocks:{storageUuid:"DS-63eed32d-6258-4b37-bda5-b3294d30e365" blocks:{block:{blockId:1692606483 genStamp:6483 numBytes:5299} status:RECEIVED} storage:{storageUuid:"DS-63eed32d-6258-4b37-bda5-b3294d30e365" state:NORMAL storageType:DISK}}
*/

func opfsUpdateBlockStatus(block *hdfs.ExtendedBlockProto, storageId string, totalSize int, status, deleteHit string) {
	hclient := globalDatanodeSys.getNamenode()
	opts := hclient.RpcServerConnector.GetOptions()
	opts.AlwaysRetry = true
	client := NewNamenodeRpc(namenodeRpcOptions{
		opts: &opts,
	})
	client.reg = hclient.reg
	client.poolId = hclient.poolId
	var s hdsp.ReceivedDeletedBlockInfoProto_BlockStatus
	switch status {
	case hdsp.ReceivedDeletedBlockInfoProto_RECEIVING.String():
		s = hdsp.ReceivedDeletedBlockInfoProto_RECEIVING
	case hdsp.ReceivedDeletedBlockInfoProto_RECEIVED.String():
		s = hdsp.ReceivedDeletedBlockInfoProto_RECEIVED
	case hdsp.ReceivedDeletedBlockInfoProto_DELETED.String():
		s = hdsp.ReceivedDeletedBlockInfoProto_DELETED
	default:
		panic(fmt.Errorf("status unknow %v", status))
	}

	req := &hdsp.BlockReceivedAndDeletedRequestProto {
		Registration: client.reg,
		BlockPoolId: proto.String(block.GetPoolId()),
		Blocks: []*hdsp.StorageReceivedDeletedBlocksProto {
			&hdsp.StorageReceivedDeletedBlocksProto {
				StorageUuid: proto.String(storageId),
				Blocks: []*hdsp.ReceivedDeletedBlockInfoProto {
					&hdsp.ReceivedDeletedBlockInfoProto {
						Block: &hdfs.BlockProto {
							BlockId: proto.Uint64(block.GetBlockId()),
							GenStamp: proto.Uint64(block.GetGenerationStamp()),
							NumBytes: proto.Uint64(uint64(totalSize)),
						},
						Status: s.Enum(),
						DeleteHint: proto.String(deleteHit),
					},
				},
				Storage: &hdfs.DatanodeStorageProto {
					StorageUuid: proto.String(storageId),
					State: hdfs.DatanodeStorageProto_NORMAL.Enum(),
					StorageType:hdfs.StorageTypeProto_DISK.Enum(),
				},
			},
		},
	}
	log.Printf("send received block req %v", req)

	_, err := client.blockReceivedAndDeleted(req)
	if err != nil {
		log.Printf("block %v %v %v report fail %v", block, storageId, status, err)
		return
	}
}

func putDataToFile(t *dataTask, conn net.Conn) error {
	if err := opfs.MakeDirAll(path.Dir(t.src), shareAllPerm); err != nil {
		log.Printf("create parent dir %v fail %v", t.src, err)
		return err
	}
	f, err := opfs.OpenWithCreate(t.src, os.O_RDWR|os.O_CREATE, 0)
	if err != nil {
		log.Printf("open file %v fail %v", t.src, err)
		return err
	}
	defer f.Close()
	sum := 0
	//testpackages = 0
	for {
		//log.Printf("in write data\n")
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
		/*
		log.Printf("len %v off %v seqno %v last %v err %v\n",
			len(b), off, seqno, last, err)
		*/
		if len(b) != 0 {
			wsize, err := f.WriteAt(b, off + t.off)
			/*
			if testblocks == 2 && testpackages == 1 {
				err = fmt.Errorf("test for fail")
			}
			*/
			if err != nil {
				log.Printf("write fail %v\n", err)
				sentReply(seqno, err, conn)
				continue
			}
			sum += wsize
		}
		//testpackages++
		sentReply(seqno, err, conn)
		if last {
			log.Printf("all write size %v", sum)
			go opfsUpdateBlockStatus(t.block, t.storageId, sum, "RECEIVED", "")
			break
		}
	}

	return nil
}
