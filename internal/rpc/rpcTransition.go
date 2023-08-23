package rpc

import (
	"io"
	"log"
	"errors"

	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
)

//rpc will user clientTransit becauseof send resp to client and receive req
type clientTransit interface {
	readRequest(r io.Reader) (method string, callid uint32, req proto.Message, err error)
	writeResponse(w io.Writer, method string, callid uint32, status error, resp proto.Message) error
}

type clientBasic struct {
	ops *RpcOperators
	clientid []byte
	version uint32
}

func (c *clientBasic)readRequest(r io.Reader) (method string, callid uint32, req proto.Message, err error) {
	rrh := new(hadoop.RpcRequestHeaderProto)
	rh := new(hadoop.RequestHeaderProto)
	b, err := ReadRPCHeader(r, rrh, rh)
	if err != nil {
		log.Printf("readHeader fail %v\n", err)
		return "", 0, nil, err
	}
	log.Printf("method %s, protname %s, protocol version %d\n", rh.GetMethodName(),
		rh.GetDeclaringClassProtocolName(), rh.GetClientProtocolVersion())
	ms, err := c.ops.Methods.GetMethod(rh.GetMethodName())
	if err != nil {
		log.Printf("method %v,id %v not implement", rh.GetMethodName(), rrh.GetCallId())
		return rh.GetMethodName(), uint32(rrh.GetCallId()), nil, err
	}
	m, err := ms.Dec(b)
	if err != nil {
		log.Printf("%v id %v dec req fail %v", rh.GetMethodName(), rrh.GetCallId(), err)
		return rh.GetMethodName(), uint32(rrh.GetCallId()), nil, err
	}

	return rh.GetMethodName(), uint32(rrh.GetCallId()), m, nil
}

func (c *clientBasic)writeResponse(w io.Writer, method string, callid uint32, status error, resp proto.Message) error {
	st := c.ops.ErrToStatus(status)

	rh := &hadoop.RpcResponseHeaderProto {
		CallId: proto.Uint32(callid),
		Status: st,
		ServerIpcVersionNum: proto.Uint32(c.version),
		ClientId: c.clientid,
	}

	//there is some error happen
	if status != nil {
		rh.ExceptionClassName = proto.String(c.ops.ErrToException(status))
		rh.ErrorMsg = proto.String(c.ops.ErrToMsg(status))
		rh.ErrorDetail = c.ops.ErrToDetail(status)
	}

	b, err := MakeRPCPacket(rh, resp)
	if err != nil {
		log.Printf("clientid %v method %v id %v make resp fail %v",c.clientid, method, callid, err)
		return err
	}
	_, err = w.Write(b)
	if err != nil {
		log.Printf("clientid %v method %v id %v send resp fail %v", c.clientid, method, callid, err)
		return err
	}

	return nil
}

type serverTransit interface {
	writeRequest(w io.Writer, method string, requestID int32, req proto.Message) error
	readResponse(r io.Reader, method string, requestID int32, resp proto.Message) error
}

type serverBasic struct {
	clientid []byte
	protoClass string
	protoVersion uint64
}

func newRPCRequestHeader(id int32, clientID []byte) *hadoop.RpcRequestHeaderProto {
	return &hadoop.RpcRequestHeaderProto{
		RpcKind:  hadoop.RpcKindProto_RPC_PROTOCOL_BUFFER.Enum(),
		RpcOp:    hadoop.RpcRequestHeaderProto_RPC_FINAL_PACKET.Enum(),
		CallId:   proto.Int32(id),
		ClientId: clientID,
	}
}

func (s *serverBasic) newRequestHeader(methodName string) *hadoop.RequestHeaderProto {
	return &hadoop.RequestHeaderProto {
		MethodName: proto.String(methodName),
		DeclaringClassProtocolName: proto.String(s.protoClass),
		ClientProtocolVersion:      proto.Uint64(s.protoVersion),
	}
}

func (s *serverBasic) writeRequest(w io.Writer, method string, callid int32, req proto.Message) error {
	rrh := newRPCRequestHeader(callid, s.clientid)
	rh := s.newRequestHeader(method)

	reqBytes, err := MakeRPCPacket(rrh, rh, req)
	if err != nil {
		log.Printf("fail here!!!!err %v", err)
		return err
	}

	_, err = w.Write(reqBytes)
	return err
}

var errUnexpectedSequenceNumber = errors.New("unexpected callid for reply")

func (s *serverBasic) readResponse(r io.Reader, method string, requestID int32, resp proto.Message) error {
	rrh := &hadoop.RpcResponseHeaderProto{}
	err := ReadRPCPacket(r, rrh, resp)
	if err != nil {
		return err
	} else if int32(rrh.GetCallId()) != requestID {
		return errUnexpectedSequenceNumber
	} else if rrh.GetStatus() != hadoop.RpcResponseHeaderProto_SUCCESS {
		return &RpcError{
			method:    method,
			message:   rrh.GetErrorMsg(),
			code:      int(rrh.GetErrorDetail()),
			exception: rrh.GetExceptionClassName(),
		}
	}

	return nil
}
