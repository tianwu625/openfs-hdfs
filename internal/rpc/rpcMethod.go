package rpc

import (
	"encoding/binary"
	//"log"
	"os"
	"context"

	"google.golang.org/protobuf/proto"
)

type RpcDec func([]byte) (proto.Message, error)
type RpcFunc func(context.Context, proto.Message) (proto.Message, error)

type RpcMethod struct {
	Dec  RpcDec
	Call RpcFunc
}

type RpcMethods struct {
	methods map[string]RpcMethod
}

func (r *RpcMethods) GetMethod(k string) (RpcMethod, error) {
	ms, ok := r.methods[k]
	if !ok {
		return RpcMethod{}, os.ErrNotExist
	}

	return ms, nil
}

func (r *RpcMethods) GetLen() int {
	return len(r.methods)
}

func (r *RpcMethods) Register(methods map[string]RpcMethod) {
	for k, v := range methods {
		r.methods[k] = v
	}
}

func NewRpcMethods() *RpcMethods {
	return &RpcMethods{
		methods: make(map[string]RpcMethod),
	}
}

func ParseRequest(b []byte, req proto.Message) (proto.Message, error) {
	msgLength, n := binary.Uvarint(b)
	//log.Printf("msgLength %v, n %v, b len %v\n", msgLength, n, len(b))
	b = b[n:]
	err := proto.Unmarshal(b[:msgLength], req)
	if err != nil {
		return nil, err
	}
	return req, nil
}

type RpcMap map[string]RpcMethod

func NewRpcMap() RpcMap {
	return make(RpcMap)
}

func(m RpcMap) Merge(s RpcMap) {
	for k, v := range s {
		m[k] = v
	}
}

func(m RpcMap) InKeys(key string) bool {
	for k := range m {
		if k == key {
			return true
		}
	}

	return false
}
