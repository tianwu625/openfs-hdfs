package cmd

import (
	"log"
	"net"
	"fmt"
	"context"

	hconf "github.com/openfs/openfs-hdfs/hadoopconf"
	iam "github.com/openfs/openfs-hdfs/internal/iam"
	reconf "github.com/openfs/openfs-hdfs/internal/reconfig"
	"github.com/openfs/openfs-hdfs/internal/rpc"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/fsmeta"
)

type namenodeConf struct {
	ipcPort string
	httpAddr string
	httpsAddr string
}

func NewNamenodeConf(core hconf.HadoopConf) (*namenodeConf, error) {
	ipcPort := core.ParseNamenodeIpcPort()
	if err := checkOpfsOccupy(ipcPort); err != nil {
		return nil, err
	}
	httpAddr := core.ParseNameHttpAddress()
	if err := checkAddressValid(httpAddr); err != nil {
		return nil, err
	}
	httpsAddr := core.ParseNameHttpsAddress()
	if err := checkAddressValid(httpsAddr); err != nil {
		return nil, err
	}

	return &namenodeConf {
		ipcPort: ipcPort,
		httpAddr: httpAddr,
		httpsAddr: httpsAddr,
	}, nil
}

type constConf struct {
	blockSize uint64
	bytesPerChecksum uint32
	writePacketSize uint32
	replicate uint32
	fileBufferSize uint32
	encryptDataTransfer bool
	trashInterval uint64
	crcChunkMethod string
	lsLimit int
}

type namenodeSys struct {
	cf *namenodeConf
	fs *fsmeta.OpfsHdfsFs
	mc *opfsMetaCache
	sac *serviceAclConf
	ims *iam.IAMSys
	rc *reconf.ReconfigOnline
	handshakeafters []rpc.RpcHandshakeAfterInterface
	processbefores []rpc.RpcProcessBeforeInterface
	replybefores []rpc.RpcReplyBeforeInterface
	smm *fsmeta.SafeModeManager
	ccf *constConf
}

func (sys *namenodeSys) startNamenodeInfoServer() {
	addr := sys.cf.httpAddr
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("listen fail:", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("accept")
		}
		log.Printf("addr %v get data!!!!!\n", addr)
		conn.Close()
	}
}

func (sys *namenodeSys) startNamenodeSecurityInfoServer() {
	addr := sys.cf.httpsAddr
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("listen fail:", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("accept fail:", err)
		}
		log.Printf("addr %v get data!!!!!\n", addr)
		conn.Close()
	}
}

func (sys *namenodeSys) startNamenodeIpcServer() {
	port := sys.cf.ipcPort
	globalRpcServer := rpc.NewRpcServer(
		rpc.RpcServerOptionWithNetWork(fmt.Sprintf(":%v", port), "tcp"),
		rpc.RpcServerOptionWithMethods(globalrpcMethods),
		rpc.RpcServerOptionWithRpcErrInterface(globalrpcErr),
		rpc.RpcServerOptionWithRpcHandshakeAfterInterface(sys),
		rpc.RpcServerOptionWithRpcProcessBeforeInterface(sys),
		rpc.RpcServerOptionWithRpcReplyBeforeInterface(sys),
	)
	globalRpcServer.Start()
}

func (sys *namenodeSys) registerHandshakeafters(hand rpc.RpcHandshakeAfterInterface) error {
	sys.handshakeafters = append(sys.handshakeafters, hand)
	return nil
}

func (sys *namenodeSys) registerProcessbefores(hand rpc.RpcProcessBeforeInterface) error {
	sys.processbefores = append(sys.processbefores, hand)
	return nil
}

func (sys *namenodeSys) registerReplybefores(hand rpc.RpcReplyBeforeInterface) error {
	sys.replybefores = append(sys.replybefores, hand)
	return nil
}

func (sys *namenodeSys) HandshakeAfter(client *rpc.RpcClient) error {
	for _, hand := range sys.handshakeafters {
		if err := hand.HandshakeAfter(client); err != nil {
			return err
		}
	}

	return nil
}

func (sys *namenodeSys) ProcessBefore(client *rpc.RpcClient, ctx context.Context, m proto.Message) error {
	for _, hand := range sys.processbefores {
		if err := hand.ProcessBefore(client, ctx, m); err != nil {
			return err
		}
	}

	return nil
}

func (sys *namenodeSys) ReplyBefore(client *rpc.RpcClient, ctx context.Context, req proto.Message, resp proto.Message, status error) error {
	for _, hand := range sys.replybefores {
		if err := hand.ReplyBefore(client, ctx, req, resp, status); err != nil {
			return err
		}
	}

	return nil
}


func (sys *namenodeSys) Start() {
	go sys.startNamenodeInfoServer()
	go sys.startNamenodeSecurityInfoServer()
	sys.startNamenodeIpcServer()
}

func NewNamenodeSys(core hconf.HadoopConf) *namenodeSys {
	if err := initBlocksMap(core); err != nil {
		return nil
	}
	sys := &namenodeSys {
		fs: fsmeta.InitFsMeta(),
		mc: initMetaCache(),
		handshakeafters: make([]rpc.RpcHandshakeAfterInterface, 0),
		processbefores: make([]rpc.RpcProcessBeforeInterface, 0),
		replybefores: make([]rpc.RpcReplyBeforeInterface, 0),
		ccf: &constConf {
			blockSize: core.ParseBlockSize(),
			bytesPerChecksum: uint32(core.ParseChunkSize()),
			writePacketSize: uint32(core.ParsePacketSize()),
			replicate: uint32(core.ParseDfsReplicate()),
			fileBufferSize: core.ParseFileBufferSize(),
			encryptDataTransfer: core.ParseEncryptDataTransfer(),
			trashInterval: core.ParseTrashInterval(),
			crcChunkMethod: core.ParseChunkCrcMethod(),
			lsLimit: core.ParseLsLimit(),
		},
	}
	sys.smm = fsmeta.InitSafeModeManager(
			sys.fs,
			RpcClientNamenodeFsWriteProtoV1,
			RpcClientNamenodeFsReadProtoV1,
			RpcClientNamenodeFsManageProtoV1,
		)
	if err := sys.registerProcessbefores(sys.smm); err != nil {
		return nil
	}
	var err error
	sys.cf, err = NewNamenodeConf(core)
	if err != nil {
		return nil
	}
	sys.sac, err = getClientProtoAcl(core)
	if err != nil {
		return nil
	}
	err = sys.registerHandshakeafters(sys.sac)
	if err != nil {
		return nil
	}
	sys.ims, err = getIAM(core)
	if err != nil {
		return nil
	}
	sys.rc, err = getReconfig(core)
	if err != nil {
		return nil
	}
	return sys
}

var globalnamenodeSys *namenodeSys

func getGlobalMeta() *opfsMetaCache {
	return globalnamenodeSys.mc
}

func getGlobalServiceAcl() *serviceAclConf {
	return globalnamenodeSys.sac
}

func getGlobalIAM() *iam.IAMSys {
	return globalnamenodeSys.ims
}

func getGlobalReconfig() *reconf.ReconfigOnline {
	return globalnamenodeSys.rc
}

func getGlobalSafeModeManager() *fsmeta.SafeModeManager {
	return globalnamenodeSys.smm
}

func getGlobalConstConf() *constConf {
	return globalnamenodeSys.ccf
}
