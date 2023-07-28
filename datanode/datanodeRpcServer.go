package datanode

import (
	"net"
	"log"
	"errors"

	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"github.com/openfs/openfs-hdfs/internal/rpc"
	"google.golang.org/protobuf/proto"
)

func errToRpcProtoStatus(err error) *hadoop.RpcResponseHeaderProto_RpcStatusProto {
	switch {
	case errors.Is(err, net.ErrClosed):
		return hadoop.RpcResponseHeaderProto_FATAL.Enum()
	case err != nil:
		return hadoop.RpcResponseHeaderProto_ERROR.Enum()
	default:
		return hadoop.RpcResponseHeaderProto_SUCCESS.Enum()
	}

	return hadoop.RpcResponseHeaderProto_SUCCESS.Enum()
}

func errToDetailErr(err error) *hadoop.RpcResponseHeaderProto_RpcErrorCodeProto {
	switch {
	default:
		return hadoop.RpcResponseHeaderProto_ERROR_RPC_SERVER.Enum()
	}

	return hadoop.RpcResponseHeaderProto_ERROR_RPC_SERVER.Enum()
}

func errToException(err error) string {
	switch {
	default:
		return ""
	}

	return ""
}

func errToErrMsg(err error) string {
	switch {
	default:
		return ""
	}

	return ""
}

func DoDatanodeHandshake(conn net.Conn) {
	client, err := rpc.ParseHandshake(conn)
	if err != nil {
		log.Printf("parse handshake fail %v", err)
		conn.Close()
		return
	}
	ops := &rpc.RpcOperators {
		Methods: globalrpcMethods,
		ErrToStatus: errToRpcProtoStatus,
		ErrToDetail: errToDetailErr,
		ErrToException: errToException,
		ErrToMsg: errToErrMsg,
	}
	go rpc.HandleRpc(client, ops)
}

var RpcClientDatanodeProtoV1 map[string]rpc.RpcMethod = map[string]rpc.RpcMethod {
	"refreshNamenodes": {
		Dec: refreshNamenodesDec,
		Call: refreshNamenodes,
	},
	"getVolumeReport": {
		Dec: getVolumeReportDec,
		Call: getVolumeReport,
	},
	"deleteBlockPool": {
		Dec: deleteBlockPoolDec,
		Call: deleteBlockPool,
	},
	"getBalancerBandwidth": {
		Dec: getBalancerBandwidthDec,
		Call: getBalancerBandwidth,
	},
	"shutdownDatanode": {
		Dec: shutdownDatanodeDec,
		Call: shutdownDatanode,
	},
	"evictWriters": {
		Dec: evictWritersDec,
		Call: evictWriters,
	},
	"getDatanodeInfo": {
		Dec: getDatanodeInfoDec,
		Call: getDatanodeInfo,
	},
	"triggerBlockReport": {
		Dec: triggerBlockReportDec,
		Call: triggerBlockReport,
	},
}

func parseRequest(b []byte, req proto.Message) (proto.Message, error) {
	return rpc.ParseRequest(b, req)
}

var globalrpcMethods *rpc.RpcMethods

func init() {
	globalrpcMethods = rpc.NewRpcMethods()
	globalrpcMethods.Register(RpcClientDatanodeProtoV1)
	log.Printf("datanode rpc methods len %v", globalrpcMethods.GetLen())
}
