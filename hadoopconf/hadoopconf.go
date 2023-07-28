package hadoopconf

import (
	"strings"
	"strconv"
	"errors"
	"net"
	"log"
	"fmt"
	"time"
	"net/url"

	sacl "github.com/openfs/openfs-hdfs/internal/serviceacl"
	iam  "github.com/openfs/openfs-hdfs/internal/iam"
)

type HadoopConf map[string]string


var errInvalid = errors.New("Invalid value of property")

const (
	SecurityClientProtocolAcl = "security.client.protocol.acl"
	SecurityServiceAuthDefaultAcl = "security.service.authorization.default.acl"
	SecurityClientProtocolAclBlocked = SecurityClientProtocolAcl + ".blocked"
	SecurityServiceAuthDefaultAclBlocked = SecurityServiceAuthDefaultAcl + ".blocked"
	SecurityClientProtocolHost = "security.client.protocol.hosts"
	SecurityServiceAuthDefaultHosts = "security.service.authorization.default.hosts"
	SecurityClientProtocolHostBlocked = SecurityClientProtocolHost + ".blocked"
	SecurityServiceAuthDefaultHostsBlocked = SecurityServiceAuthDefaultHosts + ".blocked"
)

func parseAuth(authstr string) ([]string, []string, error) {
	res := strings.Split(authstr, " ")
	if len(res) != 2 {
		return []string{}, []string{}, errInvalid
	}
	userstr := res[0]
	users := strings.Split(userstr, ",")
	groupstr := res[1]
	groups := strings.Split(groupstr, ",")

	return users, groups, nil
}

func parseAuthIp(authstr string) ([]net.IP, []net.IPNet, error) {
	ss := strings.Split(authstr, ",")
	ips := make([]net.IP, 0, len(ss))
	ipnets := make([]net.IPNet, 0, len(ss))
	for _, s := range ss {
		if ip := net.ParseIP(s); ip != nil {
			ips = append(ips, ip)
		} else if addrs, err := net.LookupHost(s);err == nil {
			for _, addr := range addrs {
				ip := net.ParseIP(addr)
				ips = append(ips, ip)
			}
		} else if _, ipNet, err := net.ParseCIDR(s); err == nil {
			ipnets = append(ipnets, *ipNet)
		} else {
			return ips, ipnets, errInvalid
		}
	}

	return ips, ipnets, nil
}

const (
	defaultAllowAuth = "*"
	defaultDenyAuth = ""
)

func (h HadoopConf) getValueWithDefault(key,defaultKey,defaultValue string) string {
	s, ok := h[key]
	if !ok {
		s, ok = h[defaultKey]
		if !ok {
			s = defaultValue
		}
	}

	return s
}

func (h HadoopConf) ParseClientProtocolAcl() (*sacl.ServiceAcl, error) {
	authstr := h.getValueWithDefault(SecurityClientProtocolAcl, SecurityServiceAuthDefaultAcl, defaultAllowAuth)
	users := []string{defaultAllowAuth}
	groups := []string{defaultAllowAuth}
	var err error
	if authstr != defaultAllowAuth {
		users, groups, err = parseAuth(authstr)
		if err != nil {
			return nil, err
		}
	}
	authstr = h.getValueWithDefault(SecurityClientProtocolAclBlocked, SecurityClientProtocolAclBlocked, defaultDenyAuth)
	busers := []string{""}
	bgroups := []string{""}
	if authstr != defaultDenyAuth {
		busers, bgroups, err = parseAuth(authstr)
		if err != nil {
			return nil, err
		}
	}
	hosts := defaultAllowAuth
	ips := []net.IP{}
	ipnets := []net.IPNet{}
	authstr = h.getValueWithDefault(SecurityClientProtocolHost, SecurityServiceAuthDefaultHosts, defaultAllowAuth)
	if authstr != defaultAllowAuth {
		ips, ipnets, err = parseAuthIp(authstr)
		if err != nil {
			return nil, err
		}
		hosts = defaultDenyAuth
	}

	bhosts := defaultDenyAuth
	bips := []net.IP{}
	bipnets := []net.IPNet{}
	authstr = h.getValueWithDefault(SecurityClientProtocolHostBlocked, SecurityServiceAuthDefaultHostsBlocked, defaultDenyAuth)
	if authstr != defaultDenyAuth {
		bips, bipnets, err = parseAuthIp(authstr)
		if err != nil {
			return nil, err
		}
		bhosts = defaultAllowAuth
	}

	return &sacl.ServiceAcl {
		AllowUsers: users,
		AllowGroups: groups,
		DenyUsers: busers,
		DenyGroups: bgroups,
		AllowIps: ips,
		AllowHosts: hosts,
		AllowIpNets: ipnets,
		DenyIps:bips,
		DenyHosts: bhosts,
		DenyIpNets: bipnets,
	}, nil
}

func (h HadoopConf) Merge(n HadoopConf) HadoopConf {
	for k, v := range n {
		h[k] = v
	}

	return h
}

const (
	HadoopSecurityAuth = "hadoop.security.authorization"
)

func (h HadoopConf) ParseEnableProtoAcl() bool {
	v, ok := h[HadoopSecurityAuth]
	if !ok {
		return false
	}

	b, err := strconv.ParseBool(strings.ToLower(v))
	if err != nil {
		log.Printf("parse failed value %v", v)
		return false
	}

	return b
}

const (
	HadoopSecurityGroupsCacheSecs = "hadoop.security.groups.cache.secs"
	HadoopSecurityGroupsCacheBackgroundReload = "hadoop.security.groups.cache.background.reload"
	HadoopSecurityGroupsCacheBackgroundReloadThread = "hadoop.security.groups.cache.background.reload.threads"
	HadoopSecurityGroupsNegativeCacheSecs = "hadoop.security.groups.negative-cache.secs"
	tenTimes = 10
)

func (h HadoopConf) getValue(key string) string {
	s, ok := h[key]
	if !ok {
		panic(fmt.Errorf("get %v fail", key))
	}

	return s
}

func (h HadoopConf) ParseIAMConf() (*iam.IAMSysConf, error) {
	stay, err := strconv.Atoi(h.getValue(HadoopSecurityGroupsCacheSecs))
	if err != nil {
		return nil, err
	}
	stayTime := time.Duration(stay) * time.Second
	drop := stayTime * tenTimes
	background, err := strconv.ParseBool(h.getValue(HadoopSecurityGroupsCacheBackgroundReload))
	if err != nil {
		return nil, err
	}

	thread, err := strconv.Atoi(h.getValue(HadoopSecurityGroupsCacheBackgroundReloadThread))
	if err != nil {
		return nil, err
	}
	neg, err := strconv.Atoi(h.getValue(HadoopSecurityGroupsNegativeCacheSecs))
	if err != nil {
		return nil, err
	}
	negTime := time.Duration(neg) * time.Second

	groups, err := h.ParseRootGroups()
	if err != nil {
		return nil, err
	}

	return &iam.IAMSysConf {
		Stay: stayTime,
		Drop: drop,
		BackGround: background,
		BackGroundThread: thread,
		Negative: negTime,
		RootGroups: groups,
	}, nil
}

const (
	DfsPermissionsSuperuserGroup = "dfs.permissions.superusergroup"
	CommaSeperator = ","
)

func (h HadoopConf) ParseRootGroups() ([]string, error) {
	s, ok := h[DfsPermissionsSuperuserGroup]
	if !ok {
		panic(fmt.Errorf("get %v fail", DfsPermissionsSuperuserGroup))
	}
	groups := strings.Split(s, CommaSeperator)

	return groups, nil
}

const (
	DfsPermissionEnable = "dfs.permissions.enabled"
)

func (h HadoopConf) ParseEnableCheckPermission() bool {
	s, ok := h[DfsPermissionEnable]
	if !ok {
		panic(fmt.Errorf("get %v fail", DfsPermissionEnable))
	}

	res, err := strconv.ParseBool(s)
	if err != nil {
		panic(fmt.Errorf("%v value %v can't covert to Bool", DfsPermissionEnable, s))
	}

	return res
}

const (
	FsDefaultFs = "fs.defaultFS"
)

func (h HadoopConf)ParseNamenodeIpcPort() string {
	s := h.getValue(FsDefaultFs)
	url, err := url.Parse(s)
	if err != nil {
		panic(fmt.Errorf("parse namenode port %v fail %v", s, err))
	}
	return url.Port()
}

func buildVarMap(core HadoopConf) map[string]string {
	res := make(map[string]string)

	//get [port_number] value
	res[PortVar] = core.ParseNamenodeIpcPort()

	return res
}
const (
	DfsBlockInvalidateLimit = "dfs.block.invalidate.limit"
	DfsBlockPlacementEcClassname = "dfs.block.placement.ec.classname"
	DfsBlockReplicatorClassname = "dfs.block.replicator.classname"
	DfsDatanodePeerStatsEnabled = "dfs.datanode.peer.stats.enabled"
	DfsHeartbeatInterval = "dfs.heartbeat.interval"
	DfsImageParallelLoad = "dfs.image.parallel.load"
	DfsNamenodeAvoidReadSlowDatanode = "dfs.namenode.avoid.read.slow.datanode"
	DfsNamenodeBlockPlacementPolicyExcludeSlowNodesEnabled = "dfs.namenode.block-placement-policy.exclude-slow-nodes.enabled"
	DfsNamenodeHeartBeatRecheckInterval = "dfs.namenode.heartbeat.recheck-interval"
	DfsNamenodeMaxSlowpeerCollectNodes = "dfs.namenode.max.slowpeer.collect.nodes"
	DfsNamenodeReplicationMaxStreams = "dfs.namenode.replication.max-streams"
	DfsNamenodeReplicationMaxStreamsHardLimit = "dfs.namenode.replication.max-streams-hard-limit"
	DfsNamenodeReplicationWorkMultiplierPerIteration = "dfs.namenode.replication.work.multiplier.per.iteration"
	DfsStoragePolicySatisfierMode = "dfs.storage.policy.satisfier.mode"
	FsProtectedDirectories = "fs.protected.directories"
	HadoopCallerContextEnabled = "hadoop.caller.context.enabled"
	IpcPortBackoffEnable = "ipc.[port_number].backoff.enable"
	PortVar = "[port_number]"
)

var (
	reconfigNamenodeKeys = []string {
		DfsBlockInvalidateLimit,
		DfsBlockPlacementEcClassname,
		DfsBlockReplicatorClassname,
		DfsDatanodePeerStatsEnabled,
		DfsHeartbeatInterval,
		DfsImageParallelLoad,
		DfsNamenodeAvoidReadSlowDatanode,
		DfsNamenodeBlockPlacementPolicyExcludeSlowNodesEnabled,
		DfsNamenodeHeartBeatRecheckInterval,
		DfsNamenodeMaxSlowpeerCollectNodes,
		DfsNamenodeReplicationMaxStreams,
		DfsNamenodeReplicationMaxStreamsHardLimit,
		DfsNamenodeReplicationWorkMultiplierPerIteration,
		DfsStoragePolicySatisfierMode,
		FsProtectedDirectories,
		HadoopCallerContextEnabled,
	}
	reconfigNamenodeKeysVar = map[string][]string {
		IpcPortBackoffEnable: []string{PortVar,},
	}
	globalKeysVarMap map[string]string
)

func getRealKey(k string, varslist []string) string {
	res := ""
	for _, v := range varslist {
		res = strings.ReplaceAll(k, v, globalKeysVarMap[v])
	}

	return res
}

func (h HadoopConf) ParseReconfigNamenode() (HadoopConf, error) {
	res := HadoopConf{}
	for _, k := range reconfigNamenodeKeys {
		hv, ok := h[k]
		if !ok {
			continue
		}
		res[k] = hv
	}

	for k, v := range reconfigNamenodeKeysVar {
		rk := getRealKey(k, v)
		hv, ok := h[k]
		if !ok {
			hv, ok := h[rk]
			if ok {
				res[rk] = hv
			}
			continue
		}
		v, ok := h[rk]
		if !ok {
			res[rk] = hv
			continue
		}
		res[rk] = v
	}

	return res, nil
}

func (h HadoopConf) DiffValue(n HadoopConf) HadoopConf {
	res := HadoopConf{}

	for k, v := range h {
		nv, ok := n[k]
		if !ok {
			continue
		}
		if v != nv {
			res[k] = v
		}
	}

	return res
}

func (h HadoopConf) Clone() HadoopConf {
	res := HadoopConf{}

	for k, v := range h {
		res[k] = v
	}

	return res
}

func (h HadoopConf) ParseReconfigDatanode() HadoopConf {

	return HadoopConf{}
}

const (
	DfsDatanodeAddress = "dfs.datanode.address"
)

func (h HadoopConf) ParseXferAddress() string {
	return h.getValue(DfsDatanodeAddress)
}

const DfsDatanodeIpcAddress = "dfs.datanode.ipc.address"

func (h HadoopConf) ParseDataIpcAddress() string {
	return h.getValue(DfsDatanodeIpcAddress)
}

const DfsDatanodeHttpAddress = "dfs.datanode.http.address"

func (h HadoopConf) ParseDataHttpAddress() string {
	return h.getValue(DfsDatanodeHttpAddress)
}

const DfsDatanodeHttpsAddress = "dfs.datanode.https.address"

func (h HadoopConf) ParseDataHttpsAddress() string {
	return h.getValue(DfsDatanodeHttpsAddress)
}

const DfsNamenodeHttpAddress = "dfs.namenode.http-address"

func (h HadoopConf) ParseNameHttpAddress() string {
	return h.getValue(DfsNamenodeHttpAddress)
}

const DfsNamenodeHttpsAddress = "dfs.namenode.https-address"

func (h HadoopConf) ParseNameHttpsAddress() string {
	return h.getValue(DfsNamenodeHttpsAddress)
}

const (
	kBytes = 1024
	mBytes = 1024 * kBytes
	gBytes = 1024 * mBytes
	tBytes = 1024 * gBytes
	pBytes = 1024 * tBytes
	eBytes = 1024 * pBytes
)

func getNumFromUintByte(k, v, u string) uint64 {
	n, err := strconv.ParseUint(strings.TrimSuffix(v, u), 10, 64)
	if err != nil {
		panic(fmt.Errorf("fail to get num %v, %v, %v, %v", k, v, u, err))
	}

	return n
}

func (h HadoopConf) uintByteToNum (k string) uint64 {
	v := h.getValue(k)
	switch {
	case strings.HasSuffix(v, "k"):
		return kBytes * getNumFromUintByte(k, v, "k")
	case strings.HasSuffix(v, "m"):
		return mBytes * getNumFromUintByte(k, v, "m")
	case strings.HasSuffix(v, "g"):
		return gBytes * getNumFromUintByte(k, v, "g")
	case strings.HasSuffix(v, "t"):
		return tBytes * getNumFromUintByte(k, v, "t")
	case strings.HasSuffix(v, "p"):
		return pBytes * getNumFromUintByte(k, v, "p")
	case strings.HasSuffix(v, "e"):
		return pBytes * getNumFromUintByte(k, v, "e")
	default:
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		panic(fmt.Errorf("fail to parse %v, %v, %v", k, v, err))
	}
	return n
}

const DfsDatanodeBalanceBandwidthPerSec = "dfs.datanode.balance.bandwidthPerSec"


func (h HadoopConf) ParseDatanodeBandwidth() uint64 {
	return h.uintByteToNum(DfsDatanodeBalanceBandwidthPerSec)
}

func (h HadoopConf) ParseUint64(key string) uint64 {
	s := h.getValue(key)
	res, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		panic(fmt.Errorf("%v is %v parse to uint64 fail %v", key, s, err))
	}
	return res
}

const DfsBlockSize = "dfs.blocksize"

func (h HadoopConf) ParseBlockSize() uint64 {
	return h.ParseUint64(DfsBlockSize)
}
