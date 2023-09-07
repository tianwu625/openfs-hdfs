package fsmeta

import (
	"sync"
	"path"
	"os"
	"context"
	"errors"

	"github.com/openfs/openfs-hdfs/internal/opfsconfig"
	"github.com/openfs/openfs-hdfs/internal/opfsconstant"
	"github.com/openfs/openfs-hdfs/internal/rpc"
	"google.golang.org/protobuf/proto"
)

type opfsHdfsFsMeta struct {
	Mode string `json:"mode, omitempty"`
	RestoreFailedStorage string `json: "restoreFail, omitempty"`
	AllowSnapshot bool `json: "allowSnapshot, omitempty"`
}

type OpfsHdfsFs struct {
	meta *opfsHdfsFsMeta
	*sync.Mutex
}

var globalFs *OpfsHdfsFs

const (
	hdfsFsMeta = "fsMeta"

	ModeSafe = "SAFE" //read only
	ModeNormal = "NORMAL" // read write

	OnValue = "ON"
	OffValue = "OFF"
)

func defaultFsMeta() *opfsHdfsFsMeta {
	return &opfsHdfsFsMeta {
		Mode: ModeNormal,
		RestoreFailedStorage: OnValue,
		AllowSnapshot: false,
	}
}

func (fs *OpfsHdfsFs) GetMode() string {
	fs.Lock()
	defer fs.Unlock()
	return fs.meta.Mode
}

func (fs *OpfsHdfsFs) SetMode(mode string) error{
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

func (fs *OpfsHdfsFs) SetRestoreFailedStorage(value string) error{
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

var ErrInSafeMode error = errors.New("file system in safe mode")

func (fs *OpfsHdfsFs) ProcessBefore(client *rpc.RpcClient, ctx context.Context, r proto.Message) error {
	mode := fs.GetMode()
	if mode == ModeSafe {
		return ErrInSafeMode
	}
	return nil
}

func NewHdfsFs() *OpfsHdfsFs {
	srcFsMeta := path.Join(opfsconstant.HdfsSysDir, hdfsFsMeta)
	meta := new(opfsHdfsFsMeta)
	err := opfsconfig.LoadFromConfig(srcFsMeta, meta)
	if err != nil {
		if os.IsNotExist(err) {
			meta = defaultFsMeta()
			if err := opfsconfig.SaveToConfig(srcFsMeta, meta); err != nil {
				return nil
			}
		} else {
			return nil
		}
	}

	return &OpfsHdfsFs {
		meta: meta,
		Mutex: &sync.Mutex{},
	}
}

func InitFsMeta() *OpfsHdfsFs {
	globalFs = NewHdfsFs()
	return globalFs
}

func GetGlobalFsMeta() *OpfsHdfsFs{
	return globalFs
}
