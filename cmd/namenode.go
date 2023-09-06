package cmd

import (
	"errors"
	"log"
	"net"

	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"github.com/openfs/openfs-hdfs/internal/rpc"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/servernode"
)

var globalrpcMethods *rpc.RpcMethods
var globalrpcErr *errFunc

func parseRequest(b []byte, req proto.Message) (proto.Message, error) {
	return rpc.ParseRequest(b, req)
}

var RpcClientNamenodeProtoV1 map[string]rpc.RpcMethod = map[string]rpc.RpcMethod{
	"getFileInfo": rpc.RpcMethod{
		Dec:  getFileInfoDec,
		Call: getFileInfo,
	},
	"getListing": rpc.RpcMethod{
		Dec:  getListingDec,
		Call: getListing,
	},
	"delete": rpc.RpcMethod{
		Dec:  deleteFileDec,
		Call: deleteFile,
	},
	"mkdirs": rpc.RpcMethod{
		Dec:  mkdirsDec,
		Call: mkdirs,
	},
	"rename2": rpc.RpcMethod{
		Dec:  rename2Dec,
		Call: rename2,
	},
	"setPermission": rpc.RpcMethod{
		Dec:  setPermissionDec,
		Call: setPermission,
	},
	"setOwner": rpc.RpcMethod{
		Dec:  setOwnerDec,
		Call: setOwner,
	},
	"setTimes": rpc.RpcMethod{
		Dec:  setTimesDec,
		Call: setTimes,
	},
	"truncate": rpc.RpcMethod{
		Dec:  truncateDec,
		Call: truncate,
	},
	"getFsStats": rpc.RpcMethod{
		Dec:  getFsStatsDec,
		Call: getFsStats,
	},
	"getBlockLocations": rpc.RpcMethod{
		Dec:  getBlockLocationsDec,
		Call: getBlockLocations,
	},
	"getServerDefaults": rpc.RpcMethod{
		Dec:  getServerDefaultsDec,
		Call: getServerDefaults,
	},
	"create": rpc.RpcMethod{
		Dec:  createDec,
		Call: create,
	},
	"complete": rpc.RpcMethod{
		Dec:  completeDec,
		Call: complete,
	},
	"addBlock": rpc.RpcMethod{
		Dec:  addBlockDec,
		Call: addBlock,
	},
	"updateBlockForPipeline": rpc.RpcMethod{
		Dec:  updateBlockForPipelineDec,
		Call: updateBlockForPipeline,
	},
	"rename": rpc.RpcMethod{
		Dec:  renameDec,
		Call: rename,
	},
	"renewLease": rpc.RpcMethod{
		Dec:  renewLeaseDec,
		Call: renewLease,
	},
	"append": rpc.RpcMethod{
		Dec:  appendFileDec,
		Call: appendFile,
	},
	"updatePipeline": rpc.RpcMethod{
		Dec:  updatePipelineDec,
		Call: updatePipeline,
	},
	"concat": rpc.RpcMethod{
		Dec:  concatFileDec,
		Call: concatFile,
	},
	"getContentSummary": rpc.RpcMethod{
		Dec:  getContentSummaryDec,
		Call: getContentSummary,
	},
	"listEncryptionZones": rpc.RpcMethod{
		Dec:  listEncryptionZonesDec,
		Call: listEncryptionZones,
	},
	"modifyAclEntries": rpc.RpcMethod{
		Dec:  modifyAclEntriesDec,
		Call: modifyAclEntries,
	},
	"getAclStatus": rpc.RpcMethod{
		Dec:  getAclStatusDec,
		Call: getAclStatus,
	},
	"removeAcl": rpc.RpcMethod{
		Dec:  removeAclDec,
		Call: removeAcl,
	},
	"removeDefaultAcl": rpc.RpcMethod{
		Dec:  removeDefaultAclDec,
		Call: removeDefaultAcl,
	},
	"setAcl": rpc.RpcMethod{
		Dec:  setAclDec,
		Call: setAcl,
	},
	"getXAttrs": rpc.RpcMethod{
		Dec:  getXAttrsDec,
		Call: getXAttrs,
	},
	"setXAttr": rpc.RpcMethod{
		Dec:  setXAttrDec,
		Call: setXAttr,
	},
	"removeXAttr": rpc.RpcMethod{
		Dec:  removeXAttrDec,
		Call: removeXAttr,
	},
	"setReplication": rpc.RpcMethod{
		Dec:  setReplicationDec,
		Call: setReplication,
	},
	"setSafeMode": rpc.RpcMethod{
		Dec:  setSafeModeDec,
		Call: setSafeMode,
	},
	"getFsReplicatedBlockStats": rpc.RpcMethod{
		Dec:  getFsReplicatedBlockStatsDec,
		Call: getFsReplicatedBlockStats,
	},
	"getFsECBlockGroupStats": rpc.RpcMethod{
		Dec:  getFsECBlockGroupStatsDec,
		Call: getFsECBlockGroupStats,
	},
	"getDatanodeReport": rpc.RpcMethod{
		Dec:  getDatanodeReportDec,
		Call: getDatanodeReport,
	},
	"getSlowDatanodeReport": rpc.RpcMethod{
		Dec:  getSlowDatanodeReportDec,
		Call: getSlowDatanodeReport,
	},
	"saveNamespace": rpc.RpcMethod{
		Dec:  saveNamespaceDec,
		Call: saveNamespace,
	},
	"rollEdits": rpc.RpcMethod{
		Dec:  rollEditsDec,
		Call: rollEdits,
	},
	"restoreFailedStorage": rpc.RpcMethod{
		Dec:  restoreFailedStorageDec,
		Call: restoreFailedStorage,
	},
	"refreshNodes": rpc.RpcMethod{
		Dec:  refreshNodesDec,
		Call: refreshNodes,
	},
	"finalizeUpgrade": rpc.RpcMethod{
		Dec:  finalizeUpgradeDec,
		Call: finalizeUpgrade,
	},
	"upgradeStatus": rpc.RpcMethod{
		Dec:  upgradeStatusDec,
		Call: upgradeStatus,
	},
	"refreshServiceAcl": rpc.RpcMethod{
		Dec:  refreshServiceAclDec,
		Call: refreshServiceAcl,
	},
	"refreshUserToGroupsMappings": rpc.RpcMethod{
		Dec:  refreshUserToGroupsMappingsDec,
		Call: refreshUserToGroupsMappings,
	},
	"getGroupsForUser": rpc.RpcMethod{
		Dec:  getGroupsForUserDec,
		Call: getGroupsForUser,
	},
	"refreshSuperUserGroupsConfiguration": rpc.RpcMethod{
		Dec:  refreshSuperUserGroupsConfigurationDec,
		Call: refreshSuperUserGroupsConfiguration,
	},
	"refreshCallQueue": rpc.RpcMethod{
		Dec:  refreshCallQueueDec,
		Call: refreshCallQueue,
	},
	"getReconfigurationStatus": rpc.RpcMethod{
		Dec:  getReconfigurationStatusDec,
		Call: getReconfigurationStatus,
	},
	"listReconfigurableProperties": rpc.RpcMethod{
		Dec:  listReconfigurablePropertiesDec,
		Call: listReconfiguableProperties,
	},
	"startReconfiguration": rpc.RpcMethod{
		Dec:  startReconfigurationDec,
		Call: startReconfiguration,
	},
	"refresh": rpc.RpcMethod{
		Dec:  refreshDec,
		Call: refresh,
	},
	"setBalancerBandwidth": rpc.RpcMethod{
		Dec:  setBalancerBandwidthDec,
		Call: setBalancerBandwidth,
	},
	"allowSnapshot": rpc.RpcMethod {
		Dec: allowSnapshotDec,
		Call: allowSnapshot,
	},
	"disallowSnapshot": rpc.RpcMethod {
		Dec: disallowSnapshotDec,
		Call: disallowSnapshot,
	},
	"metaSave": rpc.RpcMethod {
		Dec: metaSaveDec,
		Call: metaSave,
	},
	"listOpenFiles": rpc.RpcMethod {
		Dec: listOpenFilesDec,
		Call: listOpenFiles,
	},
	"createSnapshot": rpc.RpcMethod {
		Dec: createSnapshotDec,
		Call: createSnapshot,
	},
	"deleteSnapshot": rpc.RpcMethod {
		Dec: deleteSnapshotDec,
		Call: deleteSnapshot,
	},
	"renameSnapshot": rpc.RpcMethod {
		Dec: renameSnapshotDec,
		Call: renameSnapshot,
	},
	"setQuota": rpc.RpcMethod {
		Dec: setQuotaDec,
		Call: setQuota,
	},
	"getSnapshottableDirListing": rpc.RpcMethod {
		Dec: getSnapshottableDirListingDec,
		Call: getSnapshottableDirListing,
	},
	"rollingUpgrade": rpc.RpcMethod {
		Dec: rollingUpgradeDec,
		Call: rollingUpgrade,
	},
	"abandonBlock": rpc.RpcMethod {
		Dec: abandonBlockDec,
		Call: abandonBlock,
	},
}

func init() {
	globalrpcMethods = rpc.NewRpcMethods()
	globalrpcMethods.Register(RpcClientNamenodeProtoV1)
	globalrpcMethods.Register(servernode.RpcDataServerProtoV1)
	globalrpcErr = &errFunc{}
	log.Printf("namenode rpc methods len %v", globalrpcMethods.GetLen())
}

type errFunc struct{}

func (e *errFunc)ErrToStatus(err error) *hadoop.RpcResponseHeaderProto_RpcStatusProto {
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

func (e *errFunc)ErrToDetail(err error) *hadoop.RpcResponseHeaderProto_RpcErrorCodeProto {
	switch {
	default:
		return hadoop.RpcResponseHeaderProto_ERROR_RPC_SERVER.Enum()
	}

	return hadoop.RpcResponseHeaderProto_ERROR_RPC_SERVER.Enum()
}

func (e *errFunc)ErrToException(err error) string {
	switch {
	case errors.Is(err, ErrNoRefresh):
		return "java.lang.IllegalArgumentException"
	case errors.Is(err, errOnlySupportRoot):
		return "org.apache.hadoop.hdfs.protocol.SnapshotException"
	case errors.Is(err, errDisallowSnapshot):
		return "org.apache.hadoop.hdfs.protocol.SnapshotException"
	default:
		return err.Error()
	}

	return ""
}

func (e *errFunc)ErrToMsg(err error) string {
	switch {
	case errors.Is(err, errOnlySupportRoot):
		return "openfs hdfs snapshot only support operate root directory"
	case errors.Is(err, errDisallowSnapshot):
		return  err.Error()
	default:
		return ""
	}

	return ""
}
