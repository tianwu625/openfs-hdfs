package rpc

import (
	"context"
	"net"
	"fmt"
	"log"
	"io"
	"errors"

	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/logger"
)

type RpcErrInterface interface {
	ErrToStatus(error) *hadoop.RpcResponseHeaderProto_RpcStatusProto
	ErrToDetail(error) *hadoop.RpcResponseHeaderProto_RpcErrorCodeProto
	ErrToException(error) string
	ErrToMsg(error) string
}

type RpcHandshakeAfterInterface interface {
	HandshakeAfter(*RpcClient) error
}

type RpcProcessBeforeInterface interface {
	ProcessBefore(*RpcClient, context.Context, proto.Message) error
}

type RpcReplyBeforeInterface interface {
	ReplyBefore(*RpcClient, context.Context, proto.Message, proto.Message, error) error
}

type RpcServer struct {
	ServerAddress string
	Network string
	stop bool
	Methods *RpcMethods
	RpcErrInterface
	RpcHandshakeAfterInterface
	RpcProcessBeforeInterface
	RpcReplyBeforeInterface
}

func (s *RpcServer)doHandshake(conn net.Conn) (*RpcClient, error) {
	client, err := ParseHandshake(conn)
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("fail to handshake %v", client))
		return nil, err
	}
	//check protocol class and version
	//if mismatch, return err
	return client, nil
}
//default process
func errToStatus(err error) *hadoop.RpcResponseHeaderProto_RpcStatusProto {
	return nil
}

func errToException(err error) string {
	return ""
}

func errToMsg(ctx context.Context, msg string) string {
	return msg
}

func errToDetail(err error) *hadoop.RpcResponseHeaderProto_RpcErrorCodeProto {
	return nil
}

func (s *RpcServer)makeRpcResponse(ctx context.Context, client *RpcClient, rrh *hadoop.RpcRequestHeaderProto, err error) *hadoop.RpcResponseHeaderProto {
	var status *hadoop.RpcResponseHeaderProto_RpcStatusProto
	if s.RpcErrInterface != nil {
		status = s.ErrToStatus(err)
	}
	if status == nil {
		status = errToStatus(err)
	}
	callid := uint32(rrh.GetCallId())
	clientid := client.ClientId

	rrrh := &hadoop.RpcResponseHeaderProto{
		CallId:              proto.Uint32(callid),
		Status:              status,
		ServerIpcVersionNum: proto.Uint32(RpcVersion),
		ClientId:            clientid,
	}

	if err != nil {
		exception := ""
		if s.RpcErrInterface != nil {
			exception = s.ErrToException(err)
		}
		if exception == "" {
			exception = errToException(err)
		}
		rrrh.ExceptionClassName = proto.String(exception)
		msg := ""
		if s.RpcErrInterface != nil {
			msg = s.ErrToMsg(err)
		}
		msg = errToMsg(ctx, msg)
		rrrh.ErrorMsg = proto.String(msg)
		var detail *hadoop.RpcResponseHeaderProto_RpcErrorCodeProto
		if s.RpcErrInterface != nil {
			detail = s.ErrToDetail(err)
		}
		if detail == nil {
			detail = errToDetail(err)
		}
		rrrh.ErrorDetail = detail
	}

	return rrrh
}

func (s *RpcServer)handleRpc(client *RpcClient) {
	for {
		rrh := new(hadoop.RpcRequestHeaderProto)
		rh := new(hadoop.RequestHeaderProto)
		b, err := ReadRPCHeader(client.Conn, rrh, rh)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				logger.LogIf(nil, fmt.Errorf("client %v readHeader fail %v", client, err))
			}
			break
		}
		if rh.GetMethodName() != "sendHeartbeat" {
			log.Printf("method %s, protname %s, protocol version %d\n", rh.GetMethodName(),
				rh.GetDeclaringClassProtocolName(), rh.GetClientProtocolVersion())
		}
		ms, err := s.Methods.GetMethod(rh.GetMethodName())
		if err != nil {
			panic(err)
		}
		m, err := ms.Dec(b)
		if err != nil {
			logger.LogIf(nil, fmt.Errorf("%v dec fail %v",client, err))
			continue
		}
		ctx := newContext(client, rh.GetMethodName(), rrh.GetCallId(), m)
		var r proto.Message
		if s.RpcProcessBeforeInterface != nil {
			err = s.ProcessBefore(client, ctx, m)
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("user define process before fail %v", err))
				goto sendReply
			}
		}
		r, err = ms.Call(ctx, m)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("call process fail %v", err))
		}
	sendReply:
		if err == nil && s.RpcReplyBeforeInterface != nil {
			err = s.ReplyBefore(client, ctx, m, r, err)
			if err != nil {
				logger.LogIf(ctx, fmt.Errorf("user define reply before fail %v", err))
			}
		}
		rrrh := s.makeRpcResponse(ctx, client, rrh, err)
		b, err = MakeRPCPacket(rrrh, r)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("make reply fail %v", err))
			continue
		}
		_, err = client.Conn.Write(b)
		if err != nil {
			logger.LogIf(ctx, fmt.Errorf("send reply fail %v", err))
			continue
		}
	}
	//logger.LogIf(nil, fmt.Errorf("client %v close", client))
	client.Conn.Close()
}

func (s *RpcServer)HandleConn(conn net.Conn) {
	c, err := s.doHandshake(conn)
	if err != nil {
		conn.Close()
		return
	}

	if s.RpcHandshakeAfterInterface != nil {
		if err := s.HandshakeAfter(c); err != nil {
			logger.LogIf(nil, fmt.Errorf("%v user define handshake fail %v", c, err))
			conn.Close()
			return
		}
	}

	go s.handleRpc(c)
}

func (s *RpcServer) Stop() {
	s.stop = true
}

func (s *RpcServer) getStop() bool {
	return s.stop
}


func (s *RpcServer) Start() {
	ln, err := net.Listen(s.Network, s.ServerAddress)
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("listen address %v network %v fail %v", s.ServerAddress, s.Network, err))
		return
	}
	defer ln.Close()

	for !s.getStop() {
		conn, err := ln.Accept()
		if err != nil {
			logger.LogIf(nil, fmt.Errorf("accept address %v network %v fail %v", s.ServerAddress, s.Network, err))
			continue
		}
		go s.HandleConn(conn)
	}
}

type Option func(*RpcServer)

func RpcServerOptionWithNetWork(addr, network string) Option {
	return func(s *RpcServer) {
		s.ServerAddress = addr
		s.Network = network
	}
}

func RpcServerOptionWithMethods(ms *RpcMethods) Option {
	return func(s *RpcServer) {
		s.Methods = ms
	}
}

func RpcServerOptionWithRpcErrInterface(i RpcErrInterface) Option {
	return func(s *RpcServer) {
		s.RpcErrInterface = i
	}
}

func RpcServerOptionWithRpcHandshakeAfterInterface(i RpcHandshakeAfterInterface) Option {
	return func(s *RpcServer) {
		s.RpcHandshakeAfterInterface = i
	}
}

func RpcServerOptionWithRpcProcessBeforeInterface(i RpcProcessBeforeInterface) Option {
	return func(s *RpcServer) {
		s.RpcProcessBeforeInterface = i
	}
}

func RpcServerOptionWithRpcReplyBeforeInterface(i RpcReplyBeforeInterface) Option {
	return func(s *RpcServer) {
		s.RpcReplyBeforeInterface = i
	}
}

func NewRpcServer(options ...Option) *RpcServer {
	s := &RpcServer{
		stop: false,
	}
	for _, option := range options {
		option(s)
	}

	if s.Methods == nil {
		logger.LogIf(nil, fmt.Errorf("RpcServer method must not be nil"))
		return nil
	}

	return s
}
