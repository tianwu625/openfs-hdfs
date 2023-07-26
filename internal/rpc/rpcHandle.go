package rpc

import (
	"log"

	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
)

type RpcOperators struct {
	Methods *RpcMethods
	ErrToStatus func(error) *hadoop.RpcResponseHeaderProto_RpcStatusProto
	ErrToDetail func(error) *hadoop.RpcResponseHeaderProto_RpcErrorCodeProto
	ErrToException func(error) string
	ErrToMsg func(error) string
}

func MakeRpcResponse(client *RpcClient, rrh *hadoop.RpcRequestHeaderProto, err error, ops *RpcOperators) *hadoop.RpcResponseHeaderProto {
	status := ops.ErrToStatus(err)
	callid := uint32(rrh.GetCallId())
	clientid := client.ClientId

	rrrh := &hadoop.RpcResponseHeaderProto{
		CallId:              proto.Uint32(callid),
		Status:              status,
		ServerIpcVersionNum: proto.Uint32(RpcVersion),
		ClientId:            clientid,
	}

	if err != nil {
		rrrh.ExceptionClassName = proto.String(ops.ErrToException(err))
		rrrh.ErrorMsg = proto.String(ops.ErrToMsg(err))
		rrrh.ErrorDetail = ops.ErrToDetail(err)
	}

	return rrrh
}

func HandleRpc(client *RpcClient, ops *RpcOperators) {
	for {
		rrh := new(hadoop.RpcRequestHeaderProto)
		rh := new(hadoop.RequestHeaderProto)
		b, err := ReadRPCHeader(client.Conn, rrh, rh)
		if err != nil {
			log.Printf("readHeader fail %v\n", err)
			break
		}
		log.Printf("method %s, protname %s, protocol version %d\n", rh.GetMethodName(),
			rh.GetDeclaringClassProtocolName(), rh.GetClientProtocolVersion())
		ms, err := ops.Methods.GetMethod(rh.GetMethodName())
		if err != nil {
			panic(err)
		}
		m, err := ms.Dec(b)
		if err != nil {
			log.Printf("dec fail %v\n", err)
			continue
		}
		r, err := ms.Call(m)
		if err != nil {
			log.Printf("call fail %v\n", err)
		}
		rrrh := MakeRpcResponse(client, rrh, err, ops)
		b, err = MakeRPCPacket(rrrh, r)
		if err != nil {
			log.Printf("enc fail %v\n", err)
			continue
		}
		_, err = client.Conn.Write(b)
		if err != nil {
			log.Printf("send message fail %v\n", err)
			continue
		}
	}
	client.Conn.Close()
}
