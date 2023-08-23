package datanode

import (
	"log"
	"net"
	"os"
	"time"
	userutils "os/user"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	hdsp "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_server"
//	"google.golang.org/protobuf/proto"

	"github.com/openfs/openfs-hdfs/internal/rpc"
	"github.com/openfs/openfs-hdfs/hadoopconf"
)

type namenodeRpc struct {
	reg *hdsp.DatanodeRegistrationProto
	poolId string
	*rpc.RpcServerConnector
}

func (n *namenodeRpc) versionRequest() (*hdfs.VersionResponseProto, error) {
	req := new(hdfs.VersionRequestProto)
	resp := new(hdfs.VersionResponseProto)
	log.Printf("req %v, resp %v, n %v", req, resp, n)
	if err := n.Execute("versionRequest", req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func (n *namenodeRpc) registerDatanode(reg *hdsp.DatanodeRegistrationProto) (*hdsp.DatanodeRegistrationProto, error) {
	req := &hdsp.RegisterDatanodeRequestProto {
		Registration: reg,
	}
	resp := new(hdsp.RegisterDatanodeResponseProto)

	if err := n.Execute("registerDatanode", req, resp); err != nil {
		return nil, err
	}

	return resp.GetRegistration(), nil

}

func (n *namenodeRpc) sendHeartbeat(req *hdsp.HeartbeatRequestProto) (*hdsp.HeartbeatResponseProto, error) {
	resp := new(hdsp.HeartbeatResponseProto)
	if err := n.Execute("sendHeartbeat", req, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

func (n *namenodeRpc) blockReport(req *hdsp.BlockReportRequestProto) (*hdsp.BlockReportResponseProto, error) {
	resp := new(hdsp.BlockReportResponseProto)
	if err := n.Execute("blockReport", req, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

func (n *namenodeRpc) blockReceivedAndDeleted (req *hdsp.BlockReceivedAndDeletedRequestProto) (*hdsp.BlockReceivedAndDeletedResponseProto,error) {
	resp := new(hdsp.BlockReceivedAndDeletedResponseProto)
	if err := n.Execute("blockReceivedAndDeleted", req, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

const (
	envKeyUser = "HADOOP_USER_NAME"
	datanodeTimeout = 5 * time.Second
	datanodeKeepAlive = 5 * time.Second
)

func namenodeRpcOptionsFromConf (core hadoopconf.HadoopConf) namenodeRpcOptions {
	user := os.Getenv("HADOOP_USER_NAME")
	if user == "" {
		u, err := userutils.Current()
		if err != nil {
			panic(err)
		}
		user = u.Username
	}

	dialFunc := (&net.Dialer {
		Timeout: datanodeTimeout,
		KeepAlive: datanodeKeepAlive,
	}).DialContext

	return namenodeRpcOptions {
		opts: &rpc.RpcServerOptions {
			Addresses: core.ParseNamenode(),
			User: user,
			DialFunc: dialFunc,
			ProtoClass: rpc.DatanodeServerProtocolClass,
			ProtoVersion: rpc.DatanodeServerProtocolVersion,
		},
	}
}

type namenodeRpcOptions struct {
	opts *rpc.RpcServerOptions
}

func NewNamenodeRpc(opts namenodeRpcOptions) *namenodeRpc {
	rpcServerConnector, err := rpc.NewRpcServerConnector(*opts.opts)
	if err != nil {
		return nil
	}
	return &namenodeRpc {
		RpcServerConnector: rpcServerConnector,
	}
}
