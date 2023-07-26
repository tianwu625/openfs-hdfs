package rpc

import (
	"io"
	"log"
	"net"

	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
)

type RpcClient struct {
	Version         byte
	RpcVersionClass byte
	AuthProtocol    byte
	ClientId        []byte
	CallId          int32
	User            string
	ClientIp        string
	Conn            net.Conn
}

// A handshake packet:
// +-----------------------------------------------------------+
// |  Header, 4 bytes ("hrpc")                                 |
// +-----------------------------------------------------------+
// |  Version, 1 byte (default verion 0x09)                    |
// +-----------------------------------------------------------+
// |  RPC service class, 1 byte (0x00)                         |
// +-----------------------------------------------------------+
// |  Auth protocol, 1 byte (Auth method None = 0x00)          |
// +-----------------------------------------------------------+
//
//  If the auth protocol is something other than 'none', the authentication
//  handshake happens here. Otherwise, everything can be sent as one packet.
//
// +-----------------------------------------------------------+
// |  uint32 length of the next two parts                      |
// +-----------------------------------------------------------+
// |  varint length + RpcRequestHeaderProto                    |
// +-----------------------------------------------------------+
// |  varint length + IpcConnectionContextProto                |
// +-----------------------------------------------------------+

const (
	rpcHeaderLen     = 7
	magicLen         = 4
	versionPos       = 4
	rpcClassPos      = 5
	authProtoPos     = 6
	RpcVersion       = 9
	serviceClass     = 0
	noneAuthProtocol = 0
)

func ParseHandshake(conn net.Conn) (*RpcClient, error) {
	rpcheader := make([]byte, rpcHeaderLen)
	if n, err := io.ReadFull(conn, rpcheader); n != rpcHeaderLen || err != nil {
		log.Printf("read rpc header fail %v size %v\n", err, n)
		return nil, err
	}

	magic := string(rpcheader[:magicLen])
	version := int(rpcheader[versionPos])
	rpcVersionClass := int(rpcheader[rpcClassPos])
	authprotocol := int(rpcheader[authProtoPos])
	log.Printf("magic %v version %v class %v protocol %v\n", magic, version, rpcVersionClass, authprotocol)
	rrh := new(hadoop.RpcRequestHeaderProto)
	cc := new(hadoop.IpcConnectionContextProto)
	if err := ReadRPCPacket(conn, rrh, cc); err != nil {
		log.Printf("rpc request head and connect context fail %v", err)
		return nil, err
	}
	log.Printf("rrh kind %v\nrpcOp %v\ncallid %v\nclientid %v\n",
		rrh.GetRpcKind(), rrh.GetRpcOp(), rrh.GetCallId(),
		string(rrh.GetClientId()))
	log.Printf("cc effect user %v\ncc real user %v\ncc protocol %v\n", cc.GetUserInfo().GetEffectiveUser(),
		cc.GetUserInfo().GetRealUser(), cc.GetProtocol())
	user := cc.GetUserInfo().GetEffectiveUser()
	if user == "" {
		user = cc.GetUserInfo().GetRealUser()
	}
	log.Printf("check user %v, addr %v", user, conn.RemoteAddr().String())
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		log.Printf("splist host port fail %v", err)
		return nil, err
	}
	log.Printf("naddr %v", host)
	return &RpcClient{
		Version:         rpcheader[versionPos],
		RpcVersionClass: rpcheader[rpcClassPos],
		AuthProtocol:    rpcheader[authProtoPos],
		ClientId:        rrh.GetClientId(),
		CallId:          rrh.GetCallId(),
		User:            user,
		ClientIp:        host,
		Conn:            conn,
	}, nil
}
