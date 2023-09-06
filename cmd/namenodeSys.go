package cmd

import (
	"log"
	"net"
	"fmt"

	hconf "github.com/openfs/openfs-hdfs/hadoopconf"
	iam "github.com/openfs/openfs-hdfs/internal/iam"
	reconf "github.com/openfs/openfs-hdfs/internal/reconfig"
	"github.com/openfs/openfs-hdfs/internal/rpc"
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

type namenodeSys struct {
	cf *namenodeConf
	fs *opfsHdfsFs
	mc *opfsMetaCache
	sac *serviceAclConf
	ims *iam.IAMSys
	rc *reconf.ReconfigOnline
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
		withNetWork(fmt.Sprintf(":%v", port), "tcp"),
		withMethods(globalrpcMethods),
		withRpcErrInterface(globalrpcErr),
		withRpcHandshakeAfterInterface(sys.sac),
	)
	globalRpcServer.Start()
}


func (sys *namenodeSys) Start() {
	go sys.startNamenodeInfoServer()
	go sys.startNamenodeSecurityInfoServer()
	sys.startNamenodeIpcServer()
}

func NewNamenodeSys(core hconf.HadoopConf) *namenodeSys {
	cf, err := NewNamenodeConf(core)
	if err != nil {
		return nil
	}
	sac, err := getClientProtoAcl(core)
	if err != nil {
		return nil
	}
	ims, err := getIAM(core)
	if err != nil {
		return nil
	}
	rc, err := getReconfig(core)
	if err != nil {
		return nil
	}
	return &namenodeSys {
		cf: cf,
		fs: initFsMeta(),
		mc: initMetaCache(),
		sac: sac,
		ims: ims,
		rc: rc,
	}
}
