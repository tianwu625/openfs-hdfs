package datanode

import (
	"fmt"
	"log"
	"hash/crc32"
	"crypto/md5"
	"encoding/binary"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

type argsFileInfo struct {
	src string //path of file
	off uint64
	size uint64
	perSize uint64
	checktype string
	bdata []byte //may be nil
}

type opfsChecksum struct {
	checksum []uint32
	off uint64 //align PerChuckSize
}

func opfsGetChecksumTable(t string) (*crc32.Table, error) {
	checksumTab := new(crc32.Table)
	switch t {
	case "CHECKSUM_CRC32":
		checksumTab = crc32.MakeTable(crc32.IEEE)
	case "CHECKSUM_CRC32C":
		checksumTab = crc32.MakeTable(crc32.Castagnoli)
	default:
		return nil, fmt.Errorf("NOTSUPPORT:not support %v", t)
	}

	return checksumTab, nil
}


func getCheckSum(finfo *argsFileInfo) (*opfsChecksum, error) {
	//Maybe get Info in Cache
	//not hit
	//check off
	off := uint64(0)
	length := uint64(0)
	if finfo.off % finfo.perSize != 0 {
		off = finfo.off - finfo.off % finfo.perSize
		length = finfo.size + finfo.off % finfo.perSize
	} else {
		off = finfo.off
		length = finfo.size
	}
	data := []byte(nil)
	if finfo.bdata != nil && length == uint64(len(finfo.bdata)) {
		data = finfo.bdata
	}
	if data == nil {
		data = make([]byte, length)
		n, err := opfsGetData(finfo.src, int64(off), data)
		if err != nil {
			return new(opfsChecksum), err
		}
		if uint64(n) != length {
			// length should be size of file from backend
			log.Printf("expect %v but get data len is %v\n", length, len(data))
			length = uint64(len(data))
		}
	}
	chunkCount := (length + finfo.perSize - 1) / finfo.perSize
	res := &opfsChecksum {
		checksum: make([]uint32, chunkCount),
		off: off,
	}

	table, err := opfsGetChecksumTable(finfo.checktype)
	if err != nil {
		return res, err
	}

	for i:= uint64(0); i < chunkCount; i++ {
		chunkoff := int(i) * int(finfo.perSize)
		v := uint32(0)
		if i + 1 == chunkCount {
			v = crc32.Checksum(data[chunkoff:], table)
		} else {
			v = crc32.Checksum(data[chunkoff:chunkoff + int(finfo.perSize)], table)
		}
		res.checksum[i] = v
	}

	return res, nil
}

func crc32ToB(sum []uint32) ([]byte, error) {
	b := make([]byte, len(sum) * 4)
	for i := 0; i < len(sum); i++ {
		off := i * 4
		binary.BigEndian.PutUint32(b[off:], sum[i])
	}
	return b, nil
}


func getCrcBytes(finfo *argsFileInfo)([]byte, error) {
	crcsum, err := getCheckSum(finfo)
	if err != nil {
		return []byte{}, err
	}
	b, err := crc32ToB(crcsum.checksum)
	if err != nil {
		return []byte{}, err
	}
	return b, nil
}


func md5crc(args *argsFileInfo) (*hdfs.BlockOpResponseProto, error) {
	res := new(hdfs.BlockOpResponseProto)
	b, err := getCrcBytes(args)
	if err != nil {
		return res, err
	}
	log.Printf("after encode checksum %v\n", b)
	md5sum := md5.New()
	md5sum.Write(b)
	md5b := md5sum.Sum(nil)
	log.Printf("md5b %v\n", md5b)
	defaultChunkSize := args.perSize

	res.Status = hdfs.Status_SUCCESS.Enum()
	res.ChecksumResponse = new(hdfs.OpBlockChecksumResponseProto)
	res.ChecksumResponse.BytesPerCrc = proto.Uint32(uint32(defaultChunkSize))
	res.ChecksumResponse.CrcPerBlock = proto.Uint64(uint64((args.size + defaultChunkSize - 1)/defaultChunkSize))
	res.ChecksumResponse.BlockChecksum = md5b
	res.ChecksumResponse.CrcType = hdfs.ChecksumTypeProto_CHECKSUM_CRC32C.Enum()
	res.ChecksumResponse.BlockChecksumOptions = new(hdfs.BlockChecksumOptionsProto)
	res.ChecksumResponse.BlockChecksumOptions.BlockChecksumType = hdfs.BlockChecksumTypeProto_MD5CRC.Enum()
	res.ChecksumResponse.BlockChecksumOptions.StripeLength = proto.Uint64(0)
	return res, nil
}

func getCompositeCrcBytes(finfo *argsFileInfo)([]byte, error) {
	off := uint64(0)
	length := uint64(0)
	if finfo.off % finfo.perSize != 0 {
		off = finfo.off - finfo.off % finfo.perSize
		length = finfo.size + finfo.off % finfo.perSize
	} else {
		off = finfo.off
		length = finfo.size
	}
	data := make([]byte, length)
	n, err := opfsGetData(finfo.src, int64(off), data)
	if err != nil {
		return []byte{}, err
	}
	length = uint64(n)
	table, err := opfsGetChecksumTable(finfo.checktype)
	if err != nil {
		return []byte{}, err
	}
	v := crc32.Checksum(data, table)
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b, nil
}

func compositecrc(args *argsFileInfo) (*hdfs.BlockOpResponseProto, error) {
	res := new(hdfs.BlockOpResponseProto)

	b, err := getCompositeCrcBytes(args)
	if err != nil {
		return res, err
	}

	defaultChunkSize := args.perSize

	res.Status = hdfs.Status_SUCCESS.Enum()
	res.ChecksumResponse = new(hdfs.OpBlockChecksumResponseProto)
	res.ChecksumResponse.BytesPerCrc = proto.Uint32(uint32(defaultChunkSize))
	res.ChecksumResponse.CrcPerBlock = proto.Uint64(uint64((args.size + defaultChunkSize - 1)/defaultChunkSize))
	res.ChecksumResponse.BlockChecksum = b
	res.ChecksumResponse.CrcType = hdfs.ChecksumTypeProto_CHECKSUM_CRC32C.Enum()
	res.ChecksumResponse.BlockChecksumOptions = new(hdfs.BlockChecksumOptionsProto)
	res.ChecksumResponse.BlockChecksumOptions.BlockChecksumType = hdfs.BlockChecksumTypeProto_COMPOSITE_CRC.Enum()
	res.ChecksumResponse.BlockChecksumOptions.StripeLength = proto.Uint64(uint64(defaultChunkSize))

	return res, nil
}

type checksumOptions struct{
	checktype string
	splitLen uint64
}

func opBlockChecksum(r *hdfs.OpBlockChecksumProto) (*hdfs.BlockOpResponseProto, error) {
	reqOptions:= r.GetBlockChecksumOptions()
	log.Printf("options %v, %v", reqOptions.GetBlockChecksumType().String(),
		reqOptions.GetStripeLength())
	options := checksumOptions {
		checktype: "MD5CRC",
		splitLen: 0,
	}
	if reqOptions != nil {
		options.checktype = reqOptions.GetBlockChecksumType().String()
		options.splitLen = reqOptions.GetStripeLength()
	}
	perSize := options.splitLen
	switch options.checktype {
	case "MD5CRC":
	case "COMPOSITE_CRC":
	default:
		return &hdfs.BlockOpResponseProto{}, fmt.Errorf("NOTSUPPORT: checktype %v", options.checktype)
	}
	if perSize == 0 {
		perSize = getDefaultChunkSize()
	}

	block := r.GetHeader().GetBlock()
	args := &argsFileInfo {
		src :block.GetPoolId(),
		off : block.GetBlockId(),
		size : block.GetNumBytes(),
		perSize : perSize,
		checktype: hdfs.ChecksumTypeProto_CHECKSUM_CRC32C.String(),
	}

	switch options.checktype {
	case "MD5CRC":
		return md5crc(args)
	case "COMPOSITE_CRC":
		return compositecrc(args)
	}

	return &hdfs.BlockOpResponseProto{}, fmt.Errorf("NOTSUPPORT: checktype %v", options.checktype)
}
