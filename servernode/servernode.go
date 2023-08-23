package servernode

import (
	"github.com/openfs/openfs-hdfs/internal/rpc"
)

var RpcDataServerProtoV1 map[string]rpc.RpcMethod = map[string]rpc.RpcMethod{
	"versionRequest": rpc.RpcMethod{
		Dec:  versionRequestDec,
		Call: versionRequest,
	},
	"registerDatanode": rpc.RpcMethod{
		Dec:  registerDatanodeDec,
		Call: registerDatanode,
	},
	"sendHeartbeat": rpc.RpcMethod {
		Dec: sendHeartbeatDec,
		Call: sendHeartbeat,
	},
	"blockReport": rpc.RpcMethod {
		Dec: blockReportDec,
		Call: blockReport,
	},
	"blockReceivedAndDeleted": rpc.RpcMethod {
		Dec: blockReceivedAndDeletedDec,
		Call: blockReceivedAndDeleted,
	},
}
