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
)

const (
	defaultNameNodePort = 8020
	defaultDataNodeXferPort = 50010
)

var globalMeta *opfsMetaCache
var globalConfEnv *hconf.HadoopConfEnv
var globalClientProtoAcl *serviceAclConf
var globalIAMSys *iam.IAMSys
var globalReconfig *reconf.ReconfigOnline

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

func startNameIpcServer(core hconf.HadoopConf) {
	//init meta cache
	globalMeta = InitAclCache()
	globalFs = InitFsMeta()
	var err error
	globalIAMSys, err = getIAM(core)
	if err != nil {
		log.Fatal("getIAM fail:", err)
	}
	globalClientProtoAcl, err = getClientProtoAcl(core)
	if err != nil {
		log.Fatal("getClientProtoAcl fail:", err)
	}
	globalReconfig, err = getReconfig(core)
	if err != nil {
		log.Fatal("get namenodeReconf fail:", err)
	}
	/*
	port := core.ParseNamenodeIpcPort()
	if err := checkOpfsOccupy(port); err != nil {
		log.Fatal("port invalid:", err)
	}
	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", port))
	*/
	//for test simple olny support defaultport

	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", defaultNameNodePort))
	if err != nil {
		log.Fatal("listen fail:", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("accept fail:", err)
		}
		go doNamenodeHandshake(conn)
	}
}

func startNameInfoServer(addr string) {
	if err := checkAddressValid(addr); err != nil {
		panic(fmt.Errorf("namenode http addr %v invalid %v", addr, err))
	}
	log.Printf("name info server start add %v", addr)
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

func startNameSecurityInfoServer(addr string) {
	if err := checkAddressValid(addr); err != nil {
		panic(fmt.Errorf("namenode https addr %v invalid %v", addr, err))
	}
	log.Printf("name Security info server start add %v", addr)
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

func StartNameNode(core hconf.HadoopConf) {
	go startNameInfoServer(core.ParseNameHttpAddress())
	go startNameSecurityInfoServer(core.ParseNameHttpsAddress())
	startNameIpcServer(core)
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

func Main(args []string) {
	opfs.Init("/")
	core := getCoreConf()
	StartDataNode(core)
	StartNameNode(core)
}
