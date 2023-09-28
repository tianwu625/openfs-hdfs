package fsmeta

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/openfs/openfs-hdfs/internal/logger"
	"github.com/openfs/openfs-hdfs/internal/opfsconfig"
	"github.com/openfs/openfs-hdfs/internal/opfsconstant"
)

type OpfsHdfsFsMeta struct {
	Mode                 string `json:"-"`
	RestoreFailedStorage string `json: "restoreFail, omitempty"`
	AllowSnapshot        bool   `json: "allowSnapshot, omitempty"`
	NameSpaceID          uint32 `json: "namespaceid, omitempty"`
	ClusterID            string `json: "clusterid, omitempty"`
	Ctime                uint64 `json:"ctime, omitempty"`
	BlockPool            string `json: "blockpool, omitempty"`
	Layout               uint32 `json: "layout, omitempty"`
}

type OpfsHdfsFs struct {
	meta *OpfsHdfsFsMeta
	*sync.RWMutex
}

func getRandInt31() int32 {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Int31()
}

func newNameSpaceID() uint32 {
	return uint32(getRandInt31())
}

func newClusterID() string {
	us := ""
	u, err := uuid.NewRandom()
	if err != nil {
		us = "bb6b2ad0-b65d-4057-a081-16fa44ea5026"
	} else {
		us = u.String()
	}
	return "CID-" + us
}

func newCtime() uint64 {
	return uint64(time.Now().UnixMilli())
}

var errNotIncludeIpv4 error = errors.New("local host not include a ipv4 address")

func getRandIpv4() (string, error) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	for _, i := range interfaces {
		addrs, err := i.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			s := addr.String()
			if strings.Contains(s, "/") {
				ip, _, err := net.ParseCIDR(s)
				if err != nil {
					continue
				}
				if ip.To4() != nil {
					return ip.String(), nil
				}
			} else {
				continue
			}
		}
	}

	return "", errNotIncludeIpv4
}

func newBlockPool(ctime uint64) string {
	ip, err := getRandIpv4()
	if err != nil {
		ip = "127.0.0.1"
	}

	return fmt.Sprintf("%s-%d-%s-%d", "BP", getRandInt31(), ip, ctime)
}

const (
	layout335 = uint32(4294967230)
)

var globalFs *OpfsHdfsFs

const (
	hdfsFsMeta = "fsMeta"

	ModeSafe   = "SAFE"   //read only
	ModeNormal = "NORMAL" // read write

	OnValue  = "ON"
	OffValue = "OFF"
)

func defaultFsMeta() *OpfsHdfsFsMeta {
	meta := &OpfsHdfsFsMeta{
		Mode:                 ModeNormal,
		RestoreFailedStorage: OnValue,
		AllowSnapshot:        false,
		NameSpaceID:          newNameSpaceID(),
		ClusterID:            newClusterID(),
		Ctime:                newCtime(),
		Layout:               layout335,
	}
	meta.BlockPool = newBlockPool(meta.Ctime)
	return meta
}

func (fs *OpfsHdfsFs) GetMode() string {
	fs.Lock()
	defer fs.Unlock()
	return fs.meta.Mode
}

func (fs *OpfsHdfsFs) SetMode(mode string) error {
	srcFsMeta := path.Join(opfsconstant.HdfsSysDir, hdfsFsMeta)
	fs.Lock()
	defer fs.Unlock()
	if fs.meta.Mode == mode {
		return nil
	}
	fs.meta.Mode = mode
	if err := opfsconfig.SaveToConfig(srcFsMeta, &fs.meta); err != nil {
		return err
	}
	return nil
}

func (fs *OpfsHdfsFs) GetRestoreFailedStorage() string {
	fs.Lock()
	defer fs.Unlock()
	return fs.meta.RestoreFailedStorage
}

func (fs *OpfsHdfsFs) SetRestoreFailedStorage(value string) error {
	srcFsMeta := path.Join(opfsconstant.HdfsSysDir, hdfsFsMeta)
	fs.Lock()
	defer fs.Unlock()
	if fs.meta.RestoreFailedStorage == value {
		return nil
	}
	fs.meta.RestoreFailedStorage = value
	if err := opfsconfig.SaveToConfig(srcFsMeta, &fs.meta); err != nil {
		return err
	}

	return nil
}

func (fs *OpfsHdfsFs) SetAllowSnapshot(value bool) error {
	srcFsMeta := path.Join(opfsconstant.HdfsSysDir, hdfsFsMeta)
	fs.Lock()
	defer fs.Unlock()
	if fs.meta.AllowSnapshot == value {
		return nil
	}
	fs.meta.AllowSnapshot = value
	if err := opfsconfig.SaveToConfig(srcFsMeta, &fs.meta); err != nil {
		return err
	}

	return nil
}

func (fs *OpfsHdfsFs) GetAllowSnapshot() bool {
	fs.Lock()
	defer fs.Unlock()

	return fs.meta.AllowSnapshot
}

func (fs *OpfsHdfsFs) GetMeta() *OpfsHdfsFsMeta {
	fs.RLock()
	defer fs.RUnlock()
	return &OpfsHdfsFsMeta{
		NameSpaceID: fs.meta.NameSpaceID,
		ClusterID:   fs.meta.ClusterID,
		Ctime:       fs.meta.Ctime,
		BlockPool:   fs.meta.BlockPool,
		Layout:      fs.meta.Layout,
	}
}

func NewHdfsFs() *OpfsHdfsFs {
	srcFsMeta := path.Join(opfsconstant.HdfsSysDir, hdfsFsMeta)
	meta := new(OpfsHdfsFsMeta)
	err := opfsconfig.LoadFromConfig(srcFsMeta, meta)
	if err != nil {
		if os.IsNotExist(err) {
			logger.LogIf(nil, fmt.Errorf("!!!!!new a fsmeta!!!!!"))
			meta = defaultFsMeta()
			if err := opfsconfig.SaveToConfig(srcFsMeta, meta); err != nil {
				return nil
			}
		} else {
			return nil
		}
	}

	return &OpfsHdfsFs{
		meta:    meta,
		RWMutex: &sync.RWMutex{},
	}
}

func InitFsMeta() *OpfsHdfsFs {
	globalFs = NewHdfsFs()
	if globalFs == nil {
		panic(fmt.Errorf("fail to init fsmeta and exit"))
	}
	return globalFs
}

func GetGlobalFsMeta() *OpfsHdfsFs {
	return globalFs
}
