package servernode

import (
	"sync"
	"log"
	"errors"
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

const EventBuffer = 128

type datanodeEntry struct {
	node *Datanode
	events chan EventRequest
}

type DatanodeMap struct {
	*sync.RWMutex
	datanodes map[string]*datanodeEntry
}

var ErrDuplicateRegister error = errors.New("duplicate register")

func (dm *DatanodeMap) Register(datanode *Datanode) error {
	dm.Lock()
	defer dm.Unlock()
	_, ok := dm.datanodes[datanode.Id.Uuid]
	if ok {
		log.Printf("register again for %v", datanode.Id.Uuid)
		return ErrDuplicateRegister
	}
	de := &datanodeEntry {
		node: datanode.Clone(),
		events: make(chan EventRequest, EventBuffer),
	}
	dm.datanodes[datanode.Id.Uuid] = de

	return nil
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

