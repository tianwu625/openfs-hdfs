package rpc

import (
	"time"
	"context"
	"net"
	"sync"
	"fmt"
	"math/rand"

	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
)

type rpcServerHost struct {
	address string
	lastError error
	lastErrorAt time.Time
}

const (
	ConnectStatusOpen = "open"
	ConnectStatusClose = "close"
)

type RpcServerConnector struct {
	ClientID []byte
	ClientName string
	User string
	Status string

	currentRequestID int32

	dialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

	conn net.Conn

	host *rpcServerHost
	hostList []*rpcServerHost
	alwaysRetry bool

	protoClass string
	protoVersion uint64

	serverTransit

	sync.Mutex
}

func newConnectionContext(user, kerberosRealm, protoClass string) *hadoop.IpcConnectionContextProto {
	if kerberosRealm != "" {
		user = user + "@" + kerberosRealm
	}

	return &hadoop.IpcConnectionContextProto {
		UserInfo: &hadoop.UserInformationProto {
			EffectiveUser: proto.String(user),
		},
		Protocol: proto.String(protoClass),
	}
}

const (
	handshakeCallID = -3
)

func (s *RpcServerConnector) doHandshake() error {
	authProtocol := noneAuthProtocol
	rpcHeader := []byte {
		0x68, 0x72, 0x70, 0x63, // "hrpc"
		RpcVersion, serviceClass, byte(authProtocol),
	}

	_, err := s.conn.Write(rpcHeader)
	if err != nil {
		return err
	}

	rrh := newRPCRequestHeader(handshakeCallID, s.ClientID)
	cc := newConnectionContext(s.User, "", s.protoClass)
	packet, err := MakeRPCPacket(rrh, cc)
	if err != nil {
		return err
	}
	_, err = s.conn.Write(packet)
	return err
}

func (s *RpcServerConnector) markFailure(err error) {
	if s.conn != nil {
		s.conn.Close()
		s.conn = nil
	}
	s.host.lastError = err
	s.host.lastErrorAt = time.Now()
}

const (
	backoffDuration = 5 * time.Second
)

func (s *RpcServerConnector) resolveConnection() error {
	if s.conn != nil {
		return nil
	}

	var err error
	if s.host != nil {
		err = s.host.lastError
	}

	for _, host := range s.hostList {
		if time.Since(host.lastErrorAt) < backoffDuration && !s.alwaysRetry{
			continue
		}

		if s.dialFunc == nil {
			s.dialFunc = (&net.Dialer{}).DialContext
		}

		s.host = host
		s.conn, err = s.dialFunc(context.Background(), "tcp", host.address)
		if err != nil {
			s.markFailure(err)
			continue
		}
		err = s.doHandshake()
		if err != nil {
			s.markFailure(err)
			continue
		}

		break
	}

	if s.conn == nil {
		if s.alwaysRetry {
			return ErrNoAvailableServer
		}
		return err
	}

	return nil
}

const (
	standbyExceptionClass = "org.apache.hadoop.ipc.StandbyException"
)

func (s *RpcServerConnector) Execute (method string, req proto.Message, resp proto.Message) error {
	s.Lock()
	defer s.Unlock()

	if s.Status == ConnectStatusClose {
		return ErrUserClose
	}
	s.currentRequestID++
	requestID := s.currentRequestID

	for {
		err := s.resolveConnection()
		if err != nil {
			return err
		}

		err = s.writeRequest(s.conn, method, requestID, req)
		if err != nil {
			s.markFailure(err)
			continue
		}
		err = s.readResponse(s.conn, method, requestID, resp)
		if err != nil {
			if nerr, ok := err.(*RpcError); ok && nerr.exception == standbyExceptionClass {
				s.markFailure(err)
				continue
			}

			return err
		}

		break

	}

	return nil
}

func (s *RpcServerConnector) SetRetry(retry bool) error {
	s.Lock()
	defer s.Unlock()
	if s.Status == ConnectStatusClose {
		return ErrUserClose
	}
	s.alwaysRetry = retry

	return nil
}

func (s *RpcServerConnector) Close() error {
	s.Lock()
	defer s.Unlock()
	s.Status = "Close"
	s.markFailure(ErrUserClose)

	return nil
}

type RpcServerOptions struct {
	Addresses []string
	User string
	DialFunc func(ctx context.Context, network, addr string)(net.Conn, error)
	ProtoClass string
	ProtoVersion uint64
	AlwaysRetry bool
}

func (s *RpcServerConnector) GetOptions() RpcServerOptions{
	s.Lock()
	defer s.Unlock()
	addresses := make([]string, 0, len(s.hostList))
	for _, h := range s.hostList {
		addresses = append(addresses, h.address)
	}

	return RpcServerOptions {
		Addresses: addresses,
		User: s.User,
		DialFunc: s.dialFunc,
		ProtoClass: s.protoClass,
		ProtoVersion: s.protoVersion,
		AlwaysRetry: s.alwaysRetry,
	}
}

const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func newClientID() []byte {
	id := make([]byte, 16)

	rand.Seed(time.Now().UTC().UnixNano())
	for i := range id {
		id[i] = chars[rand.Intn(len(chars))]
	}

	return id
}

func NewRpcServerConnector(options RpcServerOptions) (*RpcServerConnector, error) {
	hostList := make([]*rpcServerHost, len(options.Addresses))
	for i , addr := range options.Addresses {
		hostList[i] = &rpcServerHost{address: addr}
	}

	if options.User == "" {
		return nil, fmt.Errorf("user not specified")
	}

	if options.ProtoClass == "" {
		options.ProtoClass = DatanodeServerProtocolClass
	}

	if options.ProtoVersion == 0 {
		options.ProtoVersion = DatanodeServerProtocolVersion
	}

	clientId := newClientID()
	c := &RpcServerConnector {
		ClientID: clientId,
		ClientName: "openfs-hdfs-" + string(clientId),
		User: options.User,
		Status: ConnectStatusOpen,
		dialFunc: options.DialFunc,
		hostList: hostList,
		protoClass: options.ProtoClass,
		protoVersion: options.ProtoVersion,
		serverTransit: &serverBasic{
			clientid: clientId,
			protoClass: options.ProtoClass,
			protoVersion: options.ProtoVersion,
		},
		alwaysRetry: options.AlwaysRetry,
	}

	err := c.resolveConnection()
	if err != nil && !c.alwaysRetry {
		return nil, err
	}

	return c, nil
}
