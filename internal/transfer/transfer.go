// Package transfer implements wire transfer with the datanodes.
package transfer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

const (
	dataTransferVersion = 0x1c
	WriteBlockOp        = 0x50
	ReadBlockOp         = 0x51
	ChecksumBlockOp     = 0x55
)

var ErrInvalidRequest = errors.New("invalid request from datanode")
var ErrVersionMismatch = errors.New("invalid request data version")
var ErrOpNotSupport = errors.New("invalid op type")

func MakePrefixedMessage(msg proto.Message) ([]byte, error) {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	lengthBytes := make([]byte, binary.MaxVarintLen32)
	n := binary.PutUvarint(lengthBytes, uint64(len(msgBytes)))
	fmt.Printf("n %v, msgBytes %v\n", n, msgBytes)
	return append(lengthBytes[:n], msgBytes...), nil
}

func readPrefixedMessage(r io.Reader, msg proto.Message) error {
	varintBytes := make([]byte, binary.MaxVarintLen32)
	n, err := io.ReadAtLeast(r, varintBytes, binary.MaxVarintLen32)
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	} else if err != nil {
		return err
	}

	respLength, varintLength := binary.Uvarint(varintBytes)

	// The latter case happens if the datanode sent us an empty message, and
	// it we read too many bytes from the conn (i.e., we read bytes from the
	// next message). Since we can't put them back in the pipe, this is
	// irreperable. We could avoid this by reading one byte at a time until we
	// have a varint, but in practice this shouldn't happen anyway.
	if varintLength < 1 || varintLength+int(respLength) < n {
		return ErrInvalidRequest
	}

	// We may have grabbed too many bytes when reading the varint.
	respBytes := make([]byte, respLength)
	extraLength := copy(respBytes, varintBytes[varintLength:])
	_, err = io.ReadFull(r, respBytes[extraLength:])
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	} else if err != nil {
		return err
	}

	return proto.Unmarshal(respBytes, msg)
}

// A op request to a datanode:
// +-----------------------------------------------------------+
// |  Data Transfer Protocol Version, int16                    |
// +-----------------------------------------------------------+
// |  Op code, 1 byte                                          |
// +-----------------------------------------------------------+
// |  varint length + OpReadBlockProto                         |
// +-----------------------------------------------------------+
func writeBlockOpRequest(w io.Writer, op uint8, msg proto.Message) error {
	header := []byte{0x00, dataTransferVersion, op}
	msgBytes, err := MakePrefixedMessage(msg)
	if err != nil {
		return err
	}

	req := append(header, msgBytes...)
	_, err = w.Write(req)
	if err != nil {
		return err
	}

	return nil
}
const (
	opHeaderLen = 3
	versionPos = 1
	opPos = 2
)

func ReadBlockOpRequest(r io.Reader) (op uint8, msg proto.Message, err error) {
	bheader := make([]byte, opHeaderLen)
	_, err = io.ReadFull(r, bheader)
	if err != nil {
		return 0, nil, err
	}
	if bheader[versionPos] != dataTransferVersion {
		return 0, nil, ErrVersionMismatch
	}
	op = bheader[opPos]
	var req proto.Message
	switch op {
	case WriteBlockOp:
		req = new(hdfs.OpWriteBlockProto)
	case ReadBlockOp:
		req = new(hdfs.OpReadBlockProto)
	case ChecksumBlockOp:
		req = new(hdfs.OpBlockChecksumProto)
	default:
		return 0, nil, ErrOpNotSupport
	}
	err = readPrefixedMessage(r, req)
	if err != nil {
		return 0, nil, err
	}
	return op, req, nil
}

// The initial response from a datanode, in the case of reads and writes:
// +-----------------------------------------------------------+
// |  varint length + BlockOpResponseProto                     |
// +-----------------------------------------------------------+
func readBlockOpResponse(r io.Reader) (*hdfs.BlockOpResponseProto, error) {
	resp := &hdfs.BlockOpResponseProto{}
	err := readPrefixedMessage(r, resp)

	return resp, err
}

func WriteBlockOpResponse(w io.Writer, msg proto.Message) error {
	fmt.Printf("write start\n")
	msgBytes, err := MakePrefixedMessage(msg)
	if err != nil {
		return err
	}
	fmt.Printf("msgBytes %v\n", msgBytes)
	_, err = w.Write(msgBytes)
	if err != nil {
		return err
	}

	return nil
}

func getDatanodeAddress(datanode *hdfs.DatanodeIDProto, useHostname bool) string {
	var host string
	if useHostname {
		host = datanode.GetHostName()
	} else {
		host = datanode.GetIpAddr()
	}

	return fmt.Sprintf("%s:%d", host, datanode.GetXferPort())
}
