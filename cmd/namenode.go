package cmd

import (
	"encoding/binary"
	"io"
	"log"
	"net"

	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"github.com/openfs/openfs-hdfs/internal/rpc"
	"google.golang.org/protobuf/proto"
)

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
	rpcVersion       = 9
	serviceClass     = 0
	noneAuthProtocol = 0
)

type NamenodeClient struct {
	Version         byte
	RpcVersionClass byte
	AuthProtocol    byte
	ClientId        []byte
	CallId          int32
	User            string
	Conn            net.Conn
}

func doNamenodeHandshake(conn net.Conn) {
	rpcheader := make([]byte, rpcHeaderLen)
	if n, err := io.ReadFull(conn, rpcheader); n != rpcHeaderLen || err != nil {
		log.Printf("read rpc header fail %v size %v\n", err, n)
		conn.Close()
		return
	}

	magic := string(rpcheader[:magicLen])
	version := int(rpcheader[versionPos])
	rpcVersionClass := int(rpcheader[rpcClassPos])
	authprotocol := int(rpcheader[authProtoPos])
	log.Printf("magic %v version %v class %v protocol %v\n", magic, version, rpcVersionClass, authprotocol)
	rrh := new(hadoop.RpcRequestHeaderProto)
	cc := new(hadoop.IpcConnectionContextProto)
	if err := rpc.ReadRPCPacket(conn, rrh, cc); err != nil {
		log.Printf("rpc request head and connect context fail %v", err)
		conn.Close()
		return
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
	nc := NamenodeClient{
		Version:         rpcheader[versionPos],
		RpcVersionClass: rpcheader[rpcClassPos],
		AuthProtocol:    rpcheader[authProtoPos],
		ClientId:        rrh.GetClientId(),
		CallId:          rrh.GetCallId(),
		User:            user,
		Conn:            conn,
	}
	go handleRpc(&nc)
}

type rpcDec func([]byte) (proto.Message, error)
type rpcFunc func(proto.Message) (proto.Message, error)

type rpcMethod struct {
	Dec  rpcDec
	Call rpcFunc
}

type rpcMethods struct {
	methods map[string]rpcMethod
}

var globalrpcMethods rpcMethods

func parseRequest(b []byte, req proto.Message) (proto.Message, error) {
	msgLength, n := binary.Uvarint(b)
	log.Printf("msgLength %v, n %v, b len %v\n", msgLength, n, len(b))
	b = b[n:]
	err := proto.Unmarshal(b[:msgLength], req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

func getFileInfoDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetFileInfoRequestProto)
	return parseRequest(b, req)
}

func getFileInfo(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetFileInfoRequestProto)
	log.Printf("src %v\n", req.GetSrc())
	return opfsGetFileInfo(req)
}

func getListingDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetListingRequestProto)
	return parseRequest(b, req)
}

func getListing(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetListingRequestProto)
	log.Printf("src %v\nstart %v\nlocal%v\n", req.GetSrc(), req.GetStartAfter(), req.GetNeedLocation())
	return opfsGetListing(req)
}


func init() {
	globalrpcMethods = rpcMethods{
		methods: map[string]rpcMethod{
			"getFileInfo": rpcMethod{
				Dec:  getFileInfoDec,
				Call: getFileInfo,
			},
			"getListing":rpcMethod {
				Dec: getListingDec,
				Call: getListing,
			},
			"delete":rpcMethod {
				Dec: deleteFileDec,
				Call: deleteFile,
			},
			"mkdirs":rpcMethod {
				Dec: mkdirsDec,
				Call: mkdirs,
			},
			"rename2":rpcMethod {
				Dec: rename2Dec,
				Call: rename2,
			},
			"setPermission":rpcMethod {
				Dec: setPermissionDec,
				Call: setPermission,
			},
			"setOwner": rpcMethod {
				Dec: setOwnerDec,
				Call: setOwner,
			},
			"setTimes": rpcMethod {
				Dec: setTimesDec,
				Call: setTimes,
			},
			"truncate": rpcMethod {
				Dec: truncateDec,
				Call: truncate,
			},
			"getFsStats":rpcMethod {
				Dec: getFsStatsDec,
				Call: getFsStats,
			},
			"getBlockLocations":rpcMethod {
				Dec: getBlockLocationsDec,
				Call: getBlockLocations,
			},
			"getServerDefaults":rpcMethod {
				Dec: getServerDefaultsDec,
				Call:getServerDefaults,
			},
			"create":rpcMethod {
				Dec: createDec,
				Call: create,
			},
			"complete":rpcMethod {
				Dec: completeDec,
				Call: complete,
			},
			"addBlock":rpcMethod {
				Dec: addBlockDec,
				Call: addBlock,
			},
			"updateBlockForPipeline":rpcMethod {
				Dec: updateBlockForPipelineDec,
				Call:updateBlockForPipeline,
			},
			"rename":rpcMethod {
				Dec:renameDec,
				Call:rename,
			},
			"renewLease":rpcMethod {
				Dec: renewLeaseDec,
				Call:renewLease,
			},
			"append":rpcMethod {
				Dec: appendFileDec,
				Call:appendFile,
			},
			"updatePipeline":rpcMethod {
				Dec: updatePipelineDec,
				Call: updatePipeline,
			},
			"concat": rpcMethod {
				Dec: concatFileDec,
				Call: concatFile,
			},
			"getContentSummary": rpcMethod {
				Dec: getContentSummaryDec,
				Call: getContentSummary,
			},
			"listEncryptionZones":rpcMethod {
				Dec: listEncryptionZonesDec,
				Call: listEncryptionZones,
			},
			"modifyAclEntries":rpcMethod {
				Dec: modifyAclEntriesDec,
				Call: modifyAclEntries,
			},
			"getAclStatus": rpcMethod {
				Dec: getAclStatusDec,
				Call: getAclStatus,
			},
			"removeAcl": rpcMethod {
				Dec: removeAclDec,
				Call: removeAcl,
			},
			"removeDefaultAcl": rpcMethod {
				Dec: removeDefaultAclDec,
				Call: removeDefaultAcl,
			},
			"setAcl": rpcMethod {
				Dec: setAclDec,
				Call: setAcl,
			},
			"getXAttrs": rpcMethod {
				Dec: getXAttrsDec,
				Call: getXAttrs,
			},
			"setXAttr": rpcMethod {
				Dec: setXAttrDec,
				Call:setXAttr,
			},
			"removeXAttr":rpcMethod {
				Dec: removeXAttrDec,
				Call: removeXAttr,
			},
			"setReplication": rpcMethod {
				Dec: setReplicationDec,
				Call: setReplication,
			},
			"setSafeMode": rpcMethod {
				Dec: setSafeModeDec,
				Call: setSafeMode,
			},
			"getFsReplicatedBlockStats": rpcMethod {
				Dec: getFsReplicatedBlockStatsDec,
				Call: getFsReplicatedBlockStats,
			},
			"getFsECBlockGroupStats": rpcMethod {
				Dec: getFsECBlockGroupStatsDec,
				Call: getFsECBlockGroupStats,
			},
		},
	}
}

func handleRpc(nc *NamenodeClient) {
	for {
		rrh := new(hadoop.RpcRequestHeaderProto)
		rh := new(hadoop.RequestHeaderProto)
		b, err := rpc.ReadRPCHeader(nc.Conn, rrh, rh)
		if err != nil {
			log.Printf("readHeader fail %v\n", err)
			break
		}
		log.Printf("method %s, protname %s, protocol version %d\n", rh.GetMethodName(),
			rh.GetDeclaringClassProtocolName(), rh.GetClientProtocolVersion())
		ms := globalrpcMethods.methods[rh.GetMethodName()]
		m, err := ms.Dec(b)
		if err != nil {
			log.Printf("dec fail %v\n", err)
			continue
		}
		r, err := ms.Call(m)
		if err != nil {
			log.Printf("call fail %v\n", err)
			continue
		}
		status := hadoop.RpcResponseHeaderProto_SUCCESS
		rrrh := &hadoop.RpcResponseHeaderProto{
			CallId: proto.Uint32((uint32)(rrh.GetCallId())),
			Status: &status,
		}
		b, err = rpc.MakeRPCPacket(rrrh, r)
		if err != nil {
			log.Printf("enc fail %v\n", err)
			continue
		}
		_, err = nc.Conn.Write(b)
		if err != nil {
			log.Printf("send message fail %v\n", err)
			continue
		}
	}
	nc.Conn.Close()
}
