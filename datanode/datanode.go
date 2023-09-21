package datanode

import (
	"net"
	"fmt"
	"log"
	"sync"
	"time"
	"errors"
	"os"
	"path"
	"io"
	"bytes"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	xfer "github.com/openfs/openfs-hdfs/internal/transfer"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	hconf "github.com/openfs/openfs-hdfs/hadoopconf"
	hdsp "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_server"
	"github.com/openfs/openfs-hdfs/internal/rpc"
	"github.com/openfs/openfs-hdfs/internal/opfs"
	"google.golang.org/protobuf/proto"
)

type dataTask struct {
	block *hdfs.ExtendedBlockProto
	storageId string
	src string //path of file
	op string //operate
	off int64 //offset of file logic
	size int64 //length
	packstart int64 //start in block
	checkMethod string
	bytesPerCheck uint32
}

func ProcessData(t *dataTask, conn net.Conn) error {
	switch t.op {
	case "read":
		return getDataFromFile(t, conn)
	case "write":
		return putDataToFile(t, conn)
	default:
		return fmt.Errorf("invalid op %v", t.op)
	}
	return nil
}

func HandleDataXfer(conn net.Conn) {
	for {
		op, m, err := xfer.ReadBlockOpRequest(conn)
		if err != nil {
			log.Printf("readfBlockOpRequest err %v\n", err)
			break
		}
		var resp *hdfs.BlockOpResponseProto
		var t *dataTask = nil
		switch op {
		case xfer.WriteBlockOp:
			req := m.(*hdfs.OpWriteBlockProto)
			fmt.Printf("op %v req %v\n", op, req)
			resp, t, err = opWriteBlock(req)
			fmt.Printf("op %v resp %v\n", op, resp)
		case xfer.ReadBlockOp:
			req := m.(*hdfs.OpReadBlockProto)
			resp, t, err = opReadBlock(req)
		case xfer.ChecksumBlockOp:
			req := m.(*hdfs.OpBlockChecksumProto)
			fmt.Printf("op %v req %v\n", op, req)
			resp, err = opBlockChecksum(req)
		default:
			panic(fmt.Errorf("op is not support %v", op))
		}
		err = xfer.WriteBlockOpResponse(conn, resp)
		if err != nil {
			log.Printf("write BlockOpResponse %v\n", err)
			break
		}

		if err == nil && t != nil{
			err = ProcessData(t, conn)
			if err != nil {
				break
			}
		}
	}

	conn.Close()
}

type datanodeConf struct {
	retryCount uint64
	bandwidth uint64
	heartbeatInterval time.Duration
}

const (
	datanodeLayoutVersion = uint32(4294967239)
	datanodeSoftWare = "3.3.5"
)

type storageInfo struct {
	layoutVersion uint32
	namespaceId uint32
	clusterId string
	ctime uint64
}

type datanodeAttrs struct {
	softwareVersion string
}

type block struct {
	id uint64
	length uint64
	gen uint64
	state uint64
}

type blockPool struct {
	poolid string
	blocks []block
}

type datanodeConstConf struct {
	blockSize uint64
	chunkSize uint64
	packetSize uint64
}

type datanodeSys struct {
	constConf *datanodeConstConf
	*sync.RWMutex
	conf *datanodeConf
	namenode *namenodeRpc
	meta *datanodeVersion
	done chan struct{}
	id *hconf.DatanodeId
	namenodeInfo *storageInfo
	attrs *datanodeAttrs
	hbm *heartbeatManager
	pools map[string]*blockPool
}

func (sys *datanodeSys) GetBandwidth() uint64 {
	sys.RLock()
	defer sys.RUnlock()
	return sys.conf.bandwidth
}

func (sys *datanodeSys) SetBandwidth(bandwidth uint64) error {
	sys.Lock()
	defer sys.Unlock()
	sys.conf.bandwidth = bandwidth
	return nil
}

func (sys *datanodeSys) SetDataConfFromCore(core hconf.HadoopConf) error {
	sys.Lock()
	defer sys.Unlock()
	sys.conf.bandwidth = core.ParseDatanodeBandwidth()
	sys.conf.heartbeatInterval = core.ParseDfsHeartbeatInterval()

	return nil
}

func (sys *datanodeSys) GetHeartbeatInterval() time.Duration {
	sys.RLock()
	defer sys.RUnlock()
	return sys.conf.heartbeatInterval
}

func (sys *datanodeSys) SetHearbeatInterval(d time.Duration) error {
	sys.Lock()
	defer sys.Unlock()
	sys.conf.heartbeatInterval = d
	return nil
}
/*
resp info:{buildVersion:"706d88266abcee09ed78fbaa0ad5f74d818ab0e9" unused:0 blockPoolID:"BP-2038318745-192.168.21.164-1690428558063" storageInfo:{layoutVersion:4294967230 namespceID:1349015764 clusterID:"CID-77a6faeb-66f5-4743-8370-807e2413d3c1" cTime:1690428558063} softwareVersion:"3.3.5" capabilities:1 state:ACTIVE}
*/

const (
	STORAGE_BLOCK_REPORT_BUFFERS = (1 << iota)
)

func opfsReadAll(src string) ([]byte, error) {
	f, err := opfs.Open(src)
	if err != nil {
		log.Printf("src %v err %v", src, err)
		return []byte{}, err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return []byte{}, err
	}

	return b, nil
}

func loadFromConfig(src string, object interface{}) error {
	b, err := opfsReadAll(src)
	if err != nil {
		return err
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(b, object); err != nil {
		return err
        }

	return nil
}

func GetRandomFileName() string {
        u, err := uuid.NewRandom()
        if err != nil {
		log.Printf("get uuid fail for tmp file name %v", err)
        }

        return u.String()
}

func saveToConfig(src string, object interface{}) error {
	srcTmp := path.Join(hdfsSysDir, hdfsTmpDir, GetRandomFileName())

	err := opfs.MakeDirAll(path.Dir(srcTmp), os.FileMode(defaultConfigPerm))
	if err != nil {
		return err
	}
	f, err := opfs.OpenWithCreate(srcTmp, os.O_WRONLY | os.O_CREATE, os.FileMode(defaultConfigPerm))
	if err != nil {
		return err
	}
	defer f.Close()
	defer opfs.RemoveFile(srcTmp)
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	data, err := json.Marshal(object)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, bytes.NewReader(data)); err != nil {
		return err
	}

	if err := opfs.MakeDirAll(path.Dir(src), os.FileMode(defaultConfigPerm)); err != nil {
		log.Printf("mkdir fail %v", err)
		return err
	}
	if err := opfs.Rename(srcTmp, src); err != nil {
		log.Printf("rename fail %v", err)
		return err
	}

	return nil
}

func (sys *datanodeSys) GetClusterId() string {
	sys.RLock()
	defer sys.RUnlock()
	return sys.meta.ClusterId
}

func getHostname() string {
	host, err := os.Hostname()
	if err != nil {
		log.Printf("get host name fail %v", err)
		return ""
	}
	return host
}
const (
	hdfsSysDir = "/.hdfs.sys"
	hdfsDatanodeMeta = "DatanodeMeta"
	hdfsTmpDir = "tmp"
	shareAllPerm = 0777
	defaultConfigPerm = 0777
)

func (sys *datanodeSys) SetClusterId(id string) error {
	sys.Lock()
	defer sys.Unlock()
	sys.meta.ClusterId = id
	u, _ := uuid.NewRandom()
	sys.meta.Uuid = u.String()
	u, _ = uuid.NewRandom()
	sys.meta.StorageId = "DS-" + u.String()
	host := getHostname()
	if host == "" {
		panic(fmt.Errorf("hostname get fail!!!!!"))
	}
	src := path.Join(hdfsSysDir, host, hdfsDatanodeMeta)
	if err := saveToConfig(src, sys.meta); err != nil {
		return err
	}

	return nil
}

func (sys *datanodeSys) getDatanodeUuid() string {
	sys.RLock()
	defer sys.RUnlock()
	return sys.meta.Uuid
}

type datanodeVersion struct {
	ClusterId string `json:"clusterid"`
	Uuid string `json:"datanodeuuid"`
	StorageId string `json:"storageid"`
}

func (sys *datanodeSys) SetPool(poolId string, pool *blockPool) error {
	sys.Lock()
	defer sys.Unlock()
	_, ok := sys.pools[poolId]
	if ok {
		panic(fmt.Errorf("datanodeSys %v", sys))
	}
	sys.pools[poolId] = pool
	return nil
}

func (sys *datanodeSys) checkValidNamenode(resp *hdfs.VersionResponseProto) bool {
	poolId := resp.GetInfo().GetBlockPoolID()
	log.Printf("poolid %v", poolId)
	capabilities := resp.GetInfo().GetCapabilities()
	if capabilities != STORAGE_BLOCK_REPORT_BUFFERS {
		return false
	}
	if resp.GetInfo().GetState().String() != hdfs.NNHAStatusHeartbeatProto_ACTIVE.String() {
		return false
	}
	storageInfo := resp.GetInfo().GetStorageInfo()
	clusterId := storageInfo.GetClusterID()
	namespaceid := storageInfo.GetNamespceID()
	layoutVersion := storageInfo.GetLayoutVersion()
	ctime := storageInfo.GetCTime()
	log.Printf("namespaceid %v, layoutversion %v, ctime %v", namespaceid, layoutVersion, ctime)
	sysClusterId := sys.GetClusterId()
	if sysClusterId == "" {
		sys.SetClusterId(clusterId)
	} else if sysClusterId != clusterId {
		log.Printf("clusterid mismatch %v != %v", sysClusterId, clusterId)
		return false
	}
	sys.namenode.poolId = poolId
	pool := &blockPool {
		poolid: poolId,
		blocks: make([]block, 0),
	}
	sys.SetPool(poolId, pool)

	return true
}

func (sys *datanodeSys) getNamenodeStorageInfo() *storageInfo {
	sys.RLock()
	defer sys.RUnlock()
	return sys.namenodeInfo
}

func storageInfoToProto(s *storageInfo) *hdfs.StorageInfoProto {
	return &hdfs.StorageInfoProto {
		LayoutVersion: proto.Uint32(s.layoutVersion),
		NamespceID: proto.Uint32(s.namespaceId),
		ClusterID: proto.String(s.clusterId),
		CTime: proto.Uint64(s.ctime),
	}
}

/*
datanode id ipAddr:"0.0.0.0" hostName:"ec-1" datanodeUuid:"27427b10-a2fd-4583-8691-14674a022e11" xferPort:9866 infoPort:9864 ipcPort:9867 infoSecurePort:0, storageinfo layoutVersion:4294967239 namespceID:1349015764 clusterID:"12345" cTime:1690428558063, Keys isBlockTokenEnabled:false keyUpdateInterval:0 tokenLifeTime:0 currentKey:{keyId:0 expiryDate:0 keyBytes:""}, version 3.3.5
*/

func (sys *datanodeSys) getDatanodeId() *hconf.DatanodeId {
	sys.RLock()
	defer sys.RUnlock()
	return sys.id
}

func datanodeIdToProto(id *hconf.DatanodeId, uuid string) *hdfs.DatanodeIDProto{
	return &hdfs.DatanodeIDProto {
		IpAddr: proto.String(id.IpAddr),
		HostName: proto.String(id.Hostname),
		DatanodeUuid: proto.String(uuid),
		XferPort: proto.Uint32(id.XferPort),
		InfoPort: proto.Uint32(id.InfoPort),
		IpcPort: proto.Uint32(id.IpcPort),
		InfoSecurePort: proto.Uint32(id.InfoSecurePort),
	}
}
func (sys *datanodeSys) prepareRegister() (*hdsp.DatanodeRegistrationProto, error) {
	id := datanodeIdToProto(sys.getDatanodeId(), sys.getDatanodeUuid())
	s := storageInfoToProto(sys.getNamenodeStorageInfo())
	keys := &hdfs.ExportedBlockKeysProto {
		IsBlockTokenEnabled: proto.Bool(false),
		KeyUpdateInterval: proto.Uint64(0),
		TokenLifeTime: proto.Uint64(0),
		CurrentKey: &hdfs.BlockKeyProto {
			KeyId: proto.Uint32(0),
			ExpiryDate:proto.Uint64(0),
		},
	}

	return &hdsp.DatanodeRegistrationProto {
		DatanodeID: id,
		StorageInfo: s,
		Keys: keys,
		SoftwareVersion:proto.String(globalDatanodeSys.attrs.softwareVersion),
	}, nil
}

func protoToStorageInfo(s *hdfs.StorageInfoProto) *storageInfo {
	return &storageInfo {
		layoutVersion: datanodeLayoutVersion,
		namespaceId: s.GetNamespceID(),
		clusterId: s.GetClusterID(),
		ctime: s.GetCTime(),
	}
}

//do shadow copy not deep copy, please make attention with use

func (sys *datanodeSys) setNamenodeStorageInfo(s *storageInfo) error {
	sys.Lock()
	defer sys.Unlock()
	sys.namenodeInfo = s

	return nil
}

var errInvalidNamenode error = errors.New("invalid namenode")

func (sys *datanodeSys) registerSelf() error {
	resp, err := sys.namenode.versionRequest()
	if err != nil {
		return err
	}
	log.Printf("resp %v", resp)
	if !sys.checkValidNamenode(resp) {
		log.Printf("invalid namenode %v", resp)
		return errInvalidNamenode
	}
	if err := sys.setNamenodeStorageInfo(protoToStorageInfo(resp.GetInfo().GetStorageInfo())); err != nil {
		return err
	}
	reg, err := sys.prepareRegister()
	if err != nil {
		return err
	}

	respReg, err := sys.namenode.registerDatanode(reg)
	if err != nil {
		return err
	}
	log.Printf("respReg %v", respReg)
	sys.namenode.reg = respReg

	return nil
}

func (sys *datanodeSys) getReports() []*hdfs.StorageReportProto {
	res := make([]*hdfs.StorageReportProto, 0, 1)
	fsinfo, err := opfs.StatFs()
	if err != nil {
		return res
	}
	sys.RLock()
	defer sys.RUnlock()
	r := &hdfs.StorageReportProto {
		StorageUuid: proto.String(sys.meta.StorageId),
		Failed: proto.Bool(false),
		Capacity: proto.Uint64(fsinfo.Capacity),
		DfsUsed: proto.Uint64(fsinfo.Used),
		Remaining: proto.Uint64(fsinfo.Remaining),
		BlockPoolUsed: proto.Uint64(fsinfo.Capacity),
		Storage: &hdfs.DatanodeStorageProto {
			StorageUuid: proto.String(sys.meta.StorageId),
			State: hdfs.DatanodeStorageProto_NORMAL.Enum(),
			StorageType: hdfs.StorageTypeProto_DISK.Enum(),
		},
		NonDfsUsed: proto.Uint64(0),
	}
	res = append(res, r)
	return res
}
/*
registration:{datanodeID:{ipAddr:"0.0.0.0" hostName:"ec-1" datanodeUuid:"df509518-475f-4b16-b6c5-4c3a7c5bc3ee" xferPort:9866 infoPort:9864 ipcPort:9867 infoSecurePort:0} storageInfo:{layoutVersion:4294967239 namespceID:1349015764 clusterID:"CID-77a6faeb-66f5-4743-8370-807e2413d3c1" cTime:1690428558063} keys:{isBlockTokenEnabled:false keyUpdateInterval:0 tokenLifeTime:0 currentKey:{keyId:0 expiryDate:0 keyBytes:""}} softwareVersion:"3.3.5"} reports:{storageUuid:"DS-cee67721-6bc5-4b3b-825e-713fb36e4552" capacity:53660876800 dfsUsed:8192 remaining:14090162176 blockPoolUsed:8192 storage:{storageUuid:"DS-cee67721-6bc5-4b3b-825e-713fb36e4552" state:NORMAL storageType:DISK} nonDfsUsed:39570706432} xmitsInProgress:0 xceiverCount:0 failedVolumes:0 requestFullBlockReportLease:true
*/

func (sys *datanodeSys) StartCommunicateNamenode() {
	for i := 0; i < int(sys.conf.retryCount); i++ {
		err := sys.registerSelf()
		if err != nil {
			if errors.Is(err, rpc.ErrNoAvailableServer) {
				//namenode not prepare for datanode, should retry
				time.Sleep(50 * time.Millisecond)
				continue
			}
			sys.done <- struct{}{}
		}
		break
	}
	sys.hbm = NewHeartbeatManager(sys)
	sys.hbm.setFullReport(true)
	sys.hbm.start()
}

const (
	BlockMaxPerRpc = 1000
)

func (sys *datanodeSys) doFullBlockReport(leaseId uint64) {
	req := &hdsp.BlockReportRequestProto {
		Registration: sys.namenode.reg,
		BlockPoolId: proto.String(sys.namenode.poolId),
	}
	sys.RLock()
	defer sys.RUnlock()
	pool, ok := sys.pools[sys.namenode.poolId]
	if !ok {
		panic(fmt.Errorf("pool %v not exist in datanode", sys.namenode.poolId))
	}
	id := time.Now().Unix()
	maxcount := (len(pool.blocks) + BlockMaxPerRpc - 1) / BlockMaxPerRpc
	if maxcount == 0 {
		c := &hdsp.BlockReportContextProto {
			TotalRpcs: proto.Int32(1),
			CurRpc:proto.Int32(0),
			Id: proto.Int64(id),
			LeaseId: proto.Uint64(leaseId),
		}
		reports := make([]*hdsp.StorageBlockReportProto, 0, 1)
		report := &hdsp.StorageBlockReportProto {
			Storage: &hdfs.DatanodeStorageProto {
				StorageUuid: proto.String(sys.meta.StorageId),
				State: hdfs.DatanodeStorageProto_NORMAL.Enum(),
				StorageType: hdfs.StorageTypeProto_DISK.Enum(),
			},
			NumberOfBlocks: proto.Uint64(0),
		}
		reports = append(reports, report)
		req.Reports = reports
		req.Context = c
		resp, err:= sys.namenode.blockReport(req)
		if err != nil {
			//retry
			sys.hbm.setFullReport(true)
			log.Printf("block report fail %v", err)
		}
		log.Printf("resp %v", resp)
		if resp.GetCmd().GetCmdType().String() == hdsp.DatanodeCommandProto_FinalizeCommand.String() &&
		   resp.GetCmd().GetFinalizeCmd().GetBlockPoolId() == sys.namenode.poolId {
			   log.Printf("finalize poolid %v", sys.namenode.poolId)
		   }
		return
	}
	for i := 0; i <maxcount; i++ {
		c := &hdsp.BlockReportContextProto {
			TotalRpcs: proto.Int32(int32(maxcount)),
			CurRpc:proto.Int32(int32(i)),
			Id: proto.Int64(id),
			LeaseId: proto.Uint64(leaseId),
		}
		reports := make([]*hdsp.StorageBlockReportProto, 0, 1)
		blocks := make([]uint64, 0, 4 * BlockMaxPerRpc)
		for j := i * BlockMaxPerRpc; j < BlockMaxPerRpc && j < len(pool.blocks); j++ {
			blocks = append(blocks, pool.blocks[j].id)
			blocks = append(blocks, pool.blocks[j].length)
			blocks = append(blocks, pool.blocks[j].gen)
			blocks = append(blocks, pool.blocks[j].state)
		}
		report := &hdsp.StorageBlockReportProto {
			Storage: &hdfs.DatanodeStorageProto {
				StorageUuid: proto.String(sys.meta.StorageId),
				State: hdfs.DatanodeStorageProto_NORMAL.Enum(),
				StorageType: hdfs.StorageTypeProto_DISK.Enum(),
			},
			Blocks: blocks,
			NumberOfBlocks: proto.Uint64(uint64(len(pool.blocks))),
		}
		reports = append(reports, report)
		req.Reports = reports
		req.Context = c
		resp, err := sys.namenode.blockReport(req)
		if err != nil {
			sys.hbm.setFullReport(true)
			log.Printf("block report fail %v", err)
			break
		}
		log.Printf("resp %v", resp)
	}
}

func (sys *datanodeSys) getNamenode() *namenodeRpc {
	sys.RLock()
	defer sys.RUnlock()
	return sys.namenode
}

func getDatanodeIdFromConfig(core hconf.HadoopConf) *hconf.DatanodeId {
	res := core.ParseDatanodeId()

	setOpfsServiceIpToDatanodeId(res)
	if res.Hostname == "" {
		res.Hostname = getHostname()
	}

	return res
}

const (
	defaultRetry = uint64(1000)
)

func NewDatanodeSys(core hconf.HadoopConf) *datanodeSys {
	constConf := &datanodeConstConf {
		blockSize: core.ParseBlockSize(),
		chunkSize: core.ParseChunkSize(),
		packetSize: core.ParsePacketSize(),
	}
	conf := &datanodeConf {
		bandwidth: core.ParseDatanodeBandwidth(),
		heartbeatInterval: core.ParseDfsHeartbeatInterval(),
		retryCount: defaultRetry,
	}

	if conf.heartbeatInterval == time.Duration(0) {
		return nil
	}

	opt := namenodeRpcOptionsFromConf(core)
	opt.opts.AlwaysRetry = true
	//register only localhost
	opt.opts.Addresses = []string {
		"localhost:" + core.ParseNamenodeIpcPort(),
	}

	rpcClient := NewNamenodeRpc(opt)
	if rpcClient == nil {
		log.Fatal("newNamenode failed")
	}
	meta := new(datanodeVersion)
	host := getHostname()
	if host == "" {
		panic(fmt.Errorf("get hostname fail"))
	}
	src := path.Join(hdfsSysDir, host, hdfsDatanodeMeta)
	loadFromConfig(src, meta)

	sys := &datanodeSys {
		constConf: constConf,
		RWMutex: &sync.RWMutex{},
		conf: conf,
		namenode: rpcClient,
		done: make(chan struct{}),
		meta: meta,
		id: getDatanodeIdFromConfig(core),
		attrs: &datanodeAttrs {
			softwareVersion: datanodeSoftWare,
		},
		pools: make(map[string]*blockPool),
	}

	go sys.StartCommunicateNamenode()

	return sys
}

var globalDatanodeSys *datanodeSys
var globalStartTime time.Time

func DatanodeInit(core hconf.HadoopConf) error {
	globalDatanodeSys = NewDatanodeSys(core)
	globalStartTime = time.Now()
	return nil
}

func getDefaultBlockSize() uint64 {
	if globalDatanodeSys == nil {
		panic(fmt.Errorf("not init global datanodesys"))
	}
	return globalDatanodeSys.constConf.blockSize
}

func getDefaultChunkSize() uint64 {
	if globalDatanodeSys == nil {
		panic(fmt.Errorf("not init global datanodesys"))
	}
	return globalDatanodeSys.constConf.chunkSize
}

func getDefaultPacketSize() uint64 {
	if globalDatanodeSys == nil {
		panic(fmt.Errorf("not init global datanodesys"))
	}
	return globalDatanodeSys.constConf.packetSize
}
