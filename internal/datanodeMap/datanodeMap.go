package datanodeMap

import (
	"sync"
	"log"
	"errors"
	"fmt"

	"github.com/openfs/openfs-hdfs/internal/logger"
)

var (
	ErrInvalidMethod error = errors.New("invalid alloc block method")
	ErrNotSatisfyReplicate error = errors.New("not satisfy replicate number")
	ErrDuplicateRegister error = errors.New("duplicate register")
	ErrFailAlloc error = errors.New("fail to alloc datanode to store")
)

type Datanodeid struct {
	Ipaddr string
	Hostname string
	Uuid string
	Xferport uint32
	Infoport uint32
	Ipcport uint32
	Infosecureport uint32
}

type StorageInfo struct {
	LayoutVersion uint32
	NamespaceId uint32
	ClusterId string
	Ctime uint64
}

type BlockKey struct {
	Keyid uint32
	ExpiryDate uint64
	KeyBytes []byte
}

func (bk *BlockKey) Clone() *BlockKey {
	nbk := &BlockKey {
		Keyid: bk.Keyid,
		ExpiryDate: bk.ExpiryDate,
		KeyBytes: make([]byte, len(bk.KeyBytes)),
	}

	copy(nbk.KeyBytes, bk.KeyBytes)

	return nbk
}

type ExportBlockKey struct {
	IsBlockTokenEnabled bool
	KeyUpdateInterval uint64
	TokenLifeTime uint64
	CurrentKey *BlockKey
	AllKeys []*BlockKey
}

func (ebk ExportBlockKey) Clone() ExportBlockKey {
	nebk := ExportBlockKey {
		IsBlockTokenEnabled: ebk.IsBlockTokenEnabled,
		KeyUpdateInterval: ebk.KeyUpdateInterval,
		TokenLifeTime: ebk.TokenLifeTime,
		CurrentKey: ebk.CurrentKey.Clone(),
	}

	bks := make([]*BlockKey, 0, len(ebk.AllKeys))
	for _, bk := range ebk.AllKeys {
		bks = append(bks, bk.Clone())
	}
	nebk.AllKeys = bks

	return nebk
}

type EventRequest struct {
	Action string
	Values string
}

type Datanode struct {
	Id Datanodeid
	Info StorageInfo
	Keys ExportBlockKey
	SoftVersion string
}

func (dn *Datanode) Clone() *Datanode {
	return &Datanode {
		Id: dn.Id,
		Info: dn.Info,
		Keys: dn.Keys.Clone(),
		SoftVersion: dn.SoftVersion,
	}
}

func (dn *Datanode) IsLocal() bool {
	return true
}

const EventBuffer = 128
/*
{storageUuid:"DS-79a574ea-76ff-4850-88cc-e65e370184fb" failed:false capacity:24004572217344 dfsUsed:221184 remaining:24004571996160 blockPoolUsed:24004572217344 storage:{storageUuid:"DS-79a574ea-76ff-4850-88cc-e65e370184fb" state:NORMAL storageType:DISK} nonDfsUsed:0}
*/
type DataStorageInfo struct {
	Uuid string
	State string
	Failed bool
	Capacity uint64
	DfsUsed uint64
	Remaining uint64
	BlockPoolUsed uint64
	StorageType string
	NonDfsUsed uint64
}

type DatanodeStorages struct {
	Storages []*DataStorageInfo
}

type datanodeEntry struct {
	node *Datanode
	*sync.RWMutex
	storages *DatanodeStorages
	events chan EventRequest
}

func (de *datanodeEntry) UpdateStorages (reports *DatanodeStorages) error {
	de.Lock()
	defer de.Unlock()
	de.storages = reports
	return nil
}

func (de *datanodeEntry) AllocStorages(stype string) *DataStorageInfo {
	de.RLock()
	defer de.RUnlock()
	if de.storages == nil {
		return nil
	}
	for _, s := range de.storages.Storages {
		log.Printf("s %v, type %v s.StorageType == stype %v", s, stype, s.StorageType == stype)
		if s.StorageType == stype {
			ns := *s
			log.Printf("ns %v", ns)
			return &ns
		}
	}

	return nil
}

type DatanodeMap struct {
	*sync.RWMutex
	datanodes map[string]*datanodeEntry
}


func (dm *DatanodeMap) Register(datanode *Datanode) error {
	dm.Lock()
	defer dm.Unlock()
	_, ok := dm.datanodes[datanode.Id.Uuid]
	if ok {
		log.Printf("register again for %v", datanode.Id.Uuid)
		return ErrDuplicateRegister
	}
	de := &datanodeEntry {
		RWMutex: &sync.RWMutex{},
		node: datanode.Clone(),
		events: make(chan EventRequest, EventBuffer),
	}
	dm.datanodes[datanode.Id.Uuid] = de

	return nil
}

func (dm *DatanodeMap) getDatanodeEntry(uuid string) *datanodeEntry {
	dm.RLock()
	defer dm.RUnlock()
	de, ok := dm.datanodes[uuid]
	if !ok {
		return nil
	}

	return de
}

func (dm *DatanodeMap) GetDatanodeEntry(uuid string) *datanodeEntry {
	dm.RLock()
	defer dm.RUnlock()
	de, ok := dm.datanodes[uuid]
	if !ok {
		return nil
	}

	return de
}

const (
	ReplicateMethod int = iota
	OpfsEcMethod
	MaxMethod
)

type AllocMethodOptions struct {
	Method int
	Replicate int
	ReplicateMin int
	StorageType string
	Excludes []string //exclude datanode uuid
}

type DatanodeLoc struct {
	Node *Datanode
	Storage *DataStorageInfo
}

func (dm *DatanodeMap) GetDatanodeUuid() string {
	dm.RLock()
	defer dm.RUnlock()
	for _, de := range dm.datanodes {
		return de.node.Id.Uuid
	}

	return ""
}

func (dm *DatanodeMap) GetLocDatanodeUuid() string {
	return dm.GetDatanodeUuid()
}

func (dm *DatanodeMap) GetStorageUuid(datanode string) string {
	dm.RLock()
	defer dm.RUnlock()
	for _, de := range dm.datanodes {
		if de.node.Id.Uuid == datanode {
			for _, v := range de.storages.Storages {
				return v.Uuid
			}
		}
	}

	return ""
}

func (dm *DatanodeMap) GetDatanodeFromUuid(uuid string) *Datanode {
	de := dm.getDatanodeEntry(uuid)
	if de == nil {
		return nil
	}
	return de.node.Clone()
}

func (dm *DatanodeMap) GetStorageInfoFromUuid(datanode, uuid string) *DataStorageInfo {
	de := dm.getDatanodeEntry(datanode)
	if de == nil {
		return nil
	}
	for _, v := range de.storages.Storages {
		if uuid == v.Uuid {
			info := *v
			return &info
		}
	}
	return nil
}

func shouldExclude(excludes []string, entry *datanodeEntry) bool {
	if len(excludes) == 0 {
		return false
	}

	for _, ex := range excludes {
		if ex == entry.node.Id.Uuid {
			return true
		}
	}

	return false
}

func (dm *DatanodeMap) selectDatanodes(params AllocMethodOptions) ([]*DatanodeLoc, error) {
	count := params.Replicate
	res := make([]*DatanodeLoc, 0, count)
	for _, v := range dm.datanodes {
		if shouldExclude(params.Excludes, v) {
			continue
		}
		dinfo := v.AllocStorages(params.StorageType)
		if dinfo == nil {
			log.Printf("dinfo %v", dinfo)
			continue
		}
		dl := &DatanodeLoc {
			Node: v.node.Clone(),
			Storage: dinfo,
		}
		res = append(res, dl)
		count--
		if count == 0 {
			break
		}
	}
	if count != 0 {
		if params.Replicate - count >= params.ReplicateMin {
			return res, nil
		}
		return res, ErrFailAlloc
	}

	return res, nil
}

const (
	LocLocation = "localhost"
)

func (dm *DatanodeMap) selectLocDatanode(params AllocMethodOptions) ([]*DatanodeLoc, error) {
	count := params.Replicate
	res := make([]*DatanodeLoc, 0, count)
	for _, v := range dm.datanodes {
		if shouldExclude(params.Excludes, v) {
			continue
		}
		dinfo := v.AllocStorages(params.StorageType)
		if dinfo == nil {
			continue
		}
		if !v.node.IsLocal() {
			continue
		}
		dinfo.Uuid = LocLocation
		node := v.node.Clone()
		node.Id.Uuid = LocLocation
		dl := &DatanodeLoc {
			Node: node,
			Storage: dinfo,
		}
		res = append(res, dl)
		count--
		if count == 0 {
			break
		}
	}
	if count != 0 {
		logger.LogIf(nil, fmt.Errorf("fail to alloc loc datanode"))
		return res, ErrFailAlloc
	}

	return res, nil
}

func (dm *DatanodeMap) GetDatanodeWithAllocMethod(params AllocMethodOptions) ([]*DatanodeLoc, error) {
	dm.RLock()
	defer dm.RUnlock()
	if params.Method == ReplicateMethod {
		if len(dm.datanodes) < params.ReplicateMin {
			return nil, ErrNotSatisfyReplicate
		}

		return dm.selectDatanodes(params)
	}

	if params.Method == OpfsEcMethod {
		if params.Replicate > 1 {
			params.Replicate = 1
		}
		return dm.selectLocDatanode(params)
	}

	return nil, ErrInvalidMethod
}

func NewDatanodeMap() *DatanodeMap {
	return &DatanodeMap {
		RWMutex: &sync.RWMutex{},
		datanodes: make(map[string]*datanodeEntry),
	}
}

var globalDatanodeMap *DatanodeMap

func GetGlobalDatanodeMap() *DatanodeMap {
	return globalDatanodeMap
}

func init() {
	globalDatanodeMap = NewDatanodeMap()
}

