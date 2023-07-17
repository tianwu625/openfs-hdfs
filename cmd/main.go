package cmd

import (
	"fmt"
	"log"
	"net"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	"github.com/openfs/openfs-hdfs/datanode"
	hconf "github.com/openfs/openfs-hdfs/hadoopconf"
)

const (
	defaultNameNodePort = 8020
	defaultDataNodeXferPort = 50010
)

var globalMeta *opfsAclCache
var globalConfEnv *hconf.HadoopConfEnv
var globalClientProtoAcl *serviceAclConf

func getClientProtoAcl() (*serviceAclConf, error) {
	core, err := globalConfEnv.ReloadCore()
	if !core.ParseEnableProtoAcl() {
		return NewServiceAclConf(), nil
	}
	conf, err := globalConfEnv.ReloadAclService()
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

func StartNameNode() {
	//init meta cache
	globalMeta = InitAclCache()
	globalFs = InitFsMeta()
	globalConfEnv = hconf.NewHadoopConfEnv()
	var err error
	globalClientProtoAcl, err = getClientProtoAcl()
	if err != nil {
		log.Fatal("getClientProtoAcl fail %v", err)
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", defaultNameNodePort))
	if err != nil {
		log.Fatal("listen fail", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("accept")
		}
		go doNamenodeHandshake(conn)
	}
}

func startXferServer() {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", defaultDataNodeXferPort))
	if err != nil {
		log.Fatal("listen fail", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("accept")
		}
		go datanode.HandleDataXfer(conn)
	}
}

func startNodeIpcServer() {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", 50020))
	if err != nil {
		log.Fatal("listen fail", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("accept")
		}
		log.Printf("ipc %v get data!!!!!\n", 50020)
		conn.Close()
	}
}

func startDataInfoServer() {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", 50075))
	if err != nil {
		log.Fatal("listen fail", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("accept")
		}
		log.Printf("ipc %v get data!!!!!\n", 50075)
		conn.Close()
	}
}

func startDataSecurityInfoServer() {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", 50475))
	if err != nil {
		log.Fatal("listen fail", err)
	}

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatal("accept")
		}
		log.Printf("ipc %v get data!!!!!\n", 50475)
		conn.Close()
	}
}


func StartDataNode() {
	go startXferServer()
	go startNodeIpcServer()
	go startDataInfoServer()
	go startDataSecurityInfoServer()
}

func Main(args []string) {
	opfs.Init("/")
	StartDataNode()
	StartNameNode()
}
