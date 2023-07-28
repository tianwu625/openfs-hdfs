package datanode

import (
	"net"
	"fmt"
	"log"
	"sync"
	"time"

	xfer "github.com/openfs/openfs-hdfs/internal/transfer"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	hconf "github.com/openfs/openfs-hdfs/hadoopconf"
)

type dataTask struct {
	src string //path of file
	op string //operate
	off int64 //offset of file logic
	size int64 //length
	packstart int64 //start in block
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
		var t *dataTask = nil
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
			log.Printf("write BlockOpResponse %v\n", err)
			break
		}

		if err == nil && t != nil{
			err = ProcessData(t, conn)
			if err != nil {
				break
			}
		}
	}

	conn.Close()
}

type datanodeConf struct {
	bandwidth uint64
	blocksize uint64
}

type datanodeSys struct {
	*sync.RWMutex
	conf *datanodeConf
}

func (sys *datanodeSys) GetBandwidth() uint64 {
	sys.RLock()
	defer sys.RUnlock()
	return sys.conf.bandwidth
}

func (sys *datanodeSys) GetBlockSize() uint64 {
	sys.RLock()
	defer sys.RUnlock()
	return sys.conf.blocksize
}

func (sys *datanodeSys) SetBandwidth(bandwidth uint64) error {
	sys.Lock()
	defer sys.Unlock()
	sys.conf.bandwidth = bandwidth
	return nil
}

func (sys *datanodeSys) SetBlockSize(blocksize uint64) error {
	sys.Lock()
	defer sys.Unlock()
	sys.conf.blocksize = blocksize
	return nil
}

func (sys *datanodeSys) SetDataConfFromCore(core hconf.HadoopConf) error {
	sys.Lock()
	defer sys.Unlock()
	sys.conf.bandwidth = core.ParseDatanodeBandwidth()
	sys.conf.blocksize = core.ParseBlockSize()

	return nil
}

func NewDatanodeSys(core hconf.HadoopConf) *datanodeSys {
	conf := &datanodeConf {
		bandwidth: core.ParseDatanodeBandwidth(),
		blocksize: core.ParseBlockSize(),
	}

	return &datanodeSys {
		RWMutex: &sync.RWMutex{},
		conf: conf,
	}
}

var globalDatanodeSys *datanodeSys
var globalStartTime time.Time

func DatanodeInit(core hconf.HadoopConf) error {
	globalDatanodeSys = NewDatanodeSys(core)
	globalStartTime = time.Now()
	return nil
}
