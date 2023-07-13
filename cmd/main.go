package cmd

import (
	"fmt"
	"log"
	"net"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	"github.com/openfs/openfs-hdfs/datanode"
)

const (
	defaultNameNodePort = 8020
	defaultDataNodeXferPort = 50010
)

var globalMeta *opfsAclCache

func StartNameNode() {
	//init meta cache
	globalMeta = InitAclCache()
	globalFs = InitFsMeta()
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
