package datanode

import (
	"net"
	"fmt"
	"log"

	xfer "github.com/openfs/openfs-hdfs/internal/transfer"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
)

type dataTask struct {
	src string //path of file
	op string //operate
	off int64 //offset
	size int64 //length
	checkMethod string
	bytesPerCheck uint32
}

const (
	defaultBlockSize = 128 * 1024 * 1024 //128 MB
)

func ProcessData(t *dataTask, conn net.Conn) error {
	switch t.op {
	case "read":
		return getDataFromFile(t, conn)
	case "write":
		return putDataToFile(t, conn)
	default:
		return fmt.Errorf("invalid op %v", t.op)
	}
	return nil
}

func HandleDataXfer(conn net.Conn) {
	for {
		op, m, err := xfer.ReadBlockOpRequest(conn)
		if err != nil {
			log.Printf("readfBlockOp err %v\n", err)
			break
		}
		var resp *hdfs.BlockOpResponseProto
		var t *dataTask
		switch op {
		case xfer.WriteBlockOp:
			req := m.(*hdfs.OpWriteBlockProto)
			fmt.Printf("op %v req %v\n", op, req)
			resp, t, err = opWriteBlock(req)
			fmt.Printf("op %v resp %v\n", op, resp)
		case xfer.ReadBlockOp:
			req := m.(*hdfs.OpReadBlockProto)
			resp, t, err = opReadBlock(req)
		case xfer.ChecksumBlockOp:
			req := m.(*hdfs.OpBlockChecksumProto)
			fmt.Printf("op %v req %v\n", op, req)
			resp, err = opBlockChecksum(req)
		default:
			panic(fmt.Errorf("op is not support %v", op))
		}
		err = xfer.WriteBlockOpResponse(conn, resp)
		if err != nil {
			break
		}

		if err == nil {
			err = ProcessData(t, conn)
			if err != nil {
				break
			}
		}
	}

	conn.Close()
}
