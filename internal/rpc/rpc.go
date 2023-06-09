// Package rpc implements some of the lower-level functionality required to
// communicate with the namenode and datanodes.
package rpc

import (
	"encoding/binary"
	"io"
	"errors"

	"google.golang.org/protobuf/proto"
)

var errInvalidRequest = errors.New("Parse request packet fail")

func makePrefixedMessage(msg proto.Message) ([]byte, error) {
	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}

	lengthBytes := make([]byte, 10)
	n := binary.PutUvarint(lengthBytes, uint64(len(msgBytes)))
	return append(lengthBytes[:n], msgBytes...), nil
}

func MakeRPCPacket(msgs ...proto.Message) ([]byte, error) {
	packet := make([]byte, 4, 128)

	length := 0
	for _, msg := range msgs {
		b, err := makePrefixedMessage(msg)
		if err != nil {
			return nil, err
		}

		packet = append(packet, b...)
		length += len(b)
	}

	binary.BigEndian.PutUint32(packet, uint32(length))
	return packet, nil
}

func ReadRPCPacket(r io.Reader, msgs ...proto.Message) error {
	var packetLength uint32
	err := binary.Read(r, binary.BigEndian, &packetLength)
	if err != nil {
		return err
	}

	packet := make([]byte, packetLength)
	_, err = io.ReadFull(r, packet)
	if err != nil {
		return err
	}

	for _, msg := range msgs {
		// HDFS doesn't send all the response messages all the time (for example, if
		// the RpcResponseHeaderProto contains an error).
		if len(packet) == 0 {
			return errInvalidRequest
		}

		msgLength, n := binary.Uvarint(packet)
		if n <= 0 || msgLength > uint64(len(packet)) {
			return errInvalidRequest
		}

		packet = packet[n:]
		if msgLength != 0 {
			err = proto.Unmarshal(packet[:msgLength], msg)
			if err != nil {
				return err
			}

			packet = packet[msgLength:]
		}
	}

	if len(packet) > 0 {
		return errInvalidRequest
	}

	return nil
}

func ReadRPCHeader(r io.Reader, msgs ...proto.Message) ([]byte, error) {
	var packetLength uint32
	err := binary.Read(r, binary.BigEndian, &packetLength)
	if err != nil {
		return []byte{}, err
	}

	packet := make([]byte, packetLength)
	_, err = io.ReadFull(r, packet)
	if err != nil {
		return []byte{}, err
	}

	for _, msg := range msgs {
		// HDFS doesn't send all the response messages all the time (for example, if
		// the RpcResponseHeaderProto contains an error).
		if len(packet) == 0 {
			return []byte{}, errInvalidRequest
		}

		msgLength, n := binary.Uvarint(packet)
		if n <= 0 || msgLength > uint64(len(packet)) {
			return []byte{}, errInvalidRequest
		}

		packet = packet[n:]
		if msgLength != 0 {
			err = proto.Unmarshal(packet[:msgLength], msg)
			if err != nil {
				return []byte{}, err
			}

			packet = packet[msgLength:]
		}
	}

	if len(packet) <= 0 {
		return []byte{}, errInvalidRequest
	}

	return packet, nil
}
