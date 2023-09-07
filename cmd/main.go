package cmd

import (
	"fmt"
	"log"
	"net"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	"github.com/openfs/openfs-hdfs/datanode"
	hconf "github.com/openfs/openfs-hdfs/hadoopconf"
	iam "github.com/openfs/openfs-hdfs/internal/iam"
	reconf "github.com/openfs/openfs-hdfs/internal/reconfig"
	"github.com/openfs/openfs-hdfs/internal/logger"
	hc "github.com/openfs/openfs-hdfs/internal/logger/target/console"
)

const (
	defaultNameNodePort = 8020
	defaultDataNodeXferPort = 50010
)

var globalConfEnv *hconf.HadoopConfEnv

func getClientProtoAcl(core hconf.HadoopConf) (*serviceAclConf, error) {
	if !core.ParseEnableProtoAcl() {
		return NewServiceAclConf(), nil
	}
	conf, err := globalConfEnv.ReloadServiceAcl()
	if err != nil {
		return nil, err
	}
	acl, err := conf.ParseClientProtocolAcl()
	if err != nil {
		return nil, err
	}
	aclconf := NewServiceAclConf()

	err = aclconf.Set(true, acl)
	if err != nil {
		return aclconf, err
	}

	return aclconf, nil
}

func getIAM(core hconf.HadoopConf) (*iam.IAMSys, error) {
	if !core.ParseEnableProtoAcl() && !core.ParseEnableCheckPermission() {
		log.Printf("groups unneccessary to init for this case")
	}
	conf, err := core.ParseIAMConf()
	if err != nil {
		return nil, err
	}
	return iam.NewIAMSys(conf), nil
}

func getReconfig(core hconf.HadoopConf) (*reconf.ReconfigOnline, error) {
	conf, err := core.ParseReconfigNamenode()
	if err != nil {
		return nil, err
	}
	return reconf.NewReconfig(conf), nil
}

func startXferServer(addr string) {
	if err := checkAddressValid(addr); err != nil {
		panic(fmt.Errorf("xferServer addr %v invalid %v", addr, err))
	}
	log.Printf("datanode xfert start addr %v", addr)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("listen fail:", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("accept fail:", err)
		}
		go datanode.HandleDataXfer(conn)
	}
}

func startDataIpcServer(addr string) {
	if err := checkAddressValid(addr); err != nil {
		panic(fmt.Errorf("datanode http addr %v invalid %v", addr, err))
	}
	log.Printf("data ipc server start add %v", addr)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("listen fail:", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("accept")
		}
		go datanode.DoDatanodeHandshake(conn)
	}
}


func startDataInfoServer(addr string) {
	if err := checkAddressValid(addr); err != nil {
		panic(fmt.Errorf("datanode http addr %v invalid %v", addr, err))
	}
	log.Printf("data info server start add %v", addr)
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

func startDataSecurityInfoServer(addr string) {
	if err := checkAddressValid(addr); err != nil {
		panic(fmt.Errorf("datanode https addr %v invalid %v", addr, err))
	}
	log.Printf("data Security info server start add %v", addr)
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

func StartDataNode(core hconf.HadoopConf) {
	err := datanode.DatanodeInit(core)
	if err != nil {
		panic(fmt.Errorf("init datanode fail %v", err))
	}
	go startXferServer(core.ParseXferAddress())
	go startDataIpcServer(core.ParseDataIpcAddress())
	go startDataInfoServer(core.ParseDataHttpAddress())
	go startDataSecurityInfoServer(core.ParseDataHttpsAddress())
}

func getCoreConf() hconf.HadoopConf {
	globalConfEnv = hconf.NewHadoopConfEnv()
	core, err := globalConfEnv.ReloadCore()
	if err != nil {
		panic(fmt.Errorf("get core conf fail %v", err))
	}

	return core
}

func logInit() error {
	logger.Init(GOPATH, GOROOT)
	consoleLog := hc.New()
	if err := logger.AddSystemTarget(consoleLog); err != nil {
		return err
	}

	return nil
}

func Main(args []string) {
	opfs.Init("/")
	logInit()
	core := getCoreConf()
	StartDataNode(core)
	globalnamenodeSys = NewNamenodeSys(core)
	globalnamenodeSys.Start()
}
