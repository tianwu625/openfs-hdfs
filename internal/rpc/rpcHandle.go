package rpc

import (
	"log"
	"context"
	"encoding/hex"
	"fmt"

	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/logger"
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

func newContext(client *RpcClient, method string, id int32, m proto.Message) context.Context {
	ctx := context.Background()

	reqInfo := &logger.ReqInfo {
		RemoteHost: client.Conn.RemoteAddr().String(),
		Host: client.Conn.LocalAddr().String(),
		ClientID: hex.EncodeToString(client.ClientId),
		CallID: fmt.Sprintf("%d", id),
		User: client.User,
		Method: method,
	}

	pm := m.ProtoReflect()
	fields := pm.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		//log.Printf("name %v kind %v value %v", f.Name(), f.Kind(), pm.Get(f))
		reqInfo.AppendParams(string(f.Name()), pm.Get(f))
	}

	ctx = logger.SetReqInfo(ctx, reqInfo)

	return ctx
}

func HandleRpc(client *RpcClient, ops *RpcOperators) {
	for {
		rrh := new(hadoop.RpcRequestHeaderProto)
		//log.Printf("clientid %v, id %v", rrh.GetClientId(), rrh.GetCallId())
		rh := new(hadoop.RequestHeaderProto)
		b, err := ReadRPCHeader(client.Conn, rrh, rh)
		if err != nil {
			log.Printf("readHeader fail %v\n", err)
			break
		}
		if rh.GetMethodName() != "sendHeartbeat" {
			log.Printf("method %s, protname %s, protocol version %d\n", rh.GetMethodName(),
				rh.GetDeclaringClassProtocolName(), rh.GetClientProtocolVersion())
		}
		ms, err := ops.Methods.GetMethod(rh.GetMethodName())
		if err != nil {
			panic(err)
		}
		m, err := ms.Dec(b)
		if err != nil {
			log.Printf("dec fail %v\n", err)
			continue
		}
		ctx := newContext(client, rh.GetMethodName(), rrh.GetCallId(), m)
		r, err := ms.Call(ctx, m)
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
