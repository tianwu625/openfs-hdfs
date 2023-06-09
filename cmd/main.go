package cmd

import (
	"fmt"
	"log"
	"net"

	"github.com/openfs/openfs-hdfs/internal/opfs"
)

const (
	defaultPort = 8020
)

func Main(args []string) {

	opfs.Init("/")

	ln, err := net.Listen("tcp", fmt.Sprintf(":%v", defaultPort))
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
