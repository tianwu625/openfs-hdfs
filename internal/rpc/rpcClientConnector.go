package rpc

import (
	"net"
	"sync"
)

type RpcClientConnector struct {
	protoClass string
	protoVersion uint64
	clientid []byte
	user string
	ip string
	clientTransit

	sync.Mutex
	conn net.Conn
}
