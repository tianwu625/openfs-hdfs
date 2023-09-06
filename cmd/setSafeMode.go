package cmd

import (
	"context"
	"log"
	"sync"
	"path"
	"fmt"
	"os"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func setSafeModeDec(b []byte) (proto.Message, error) {
	req := new(hdfs.SetSafeModeRequestProto)
	return parseRequest(b, req)
}

func setSafeMode(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.SetSafeModeRequestProto)
	log.Printf("Action %v\nchecked %v\n", req.GetAction(), req.GetChecked())
	return opfsSetSafeMode(req)
}

type opfsHdfsFsMeta struct {
	Mode string `json:"mode, omitempty"`
	RestoreFailedStorage string `json: "restoreFail, omitempty"`
	AllowSnapshot bool `json: "allowSnapshot, omitempty"`
}

type opfsHdfsFs struct {
	meta *opfsHdfsFsMeta
	*sync.Mutex
}

var globalFs *opfsHdfsFs

const (
	hdfsFsMeta = "fsMeta"

	modeSafe = "SAFE" //read only
	modeNormal = "NORMAL" // read write

	onValue = "ON"
	offValue = "OFF"
)

func defaultFsMeta() *opfsHdfsFsMeta {
	return &opfsHdfsFsMeta {
		Mode: modeNormal,
		RestoreFailedStorage: onValue,
		AllowSnapshot: false,
	}
}

func initFsMeta() *opfsHdfsFs {
	srcFsMeta := path.Join(hdfsSysDir, hdfsFsMeta)
	meta := new(opfsHdfsFsMeta)
	err := loadFromConfig(srcFsMeta, meta)
	if err != nil {
		if os.IsNotExist(err) {
			meta = defaultFsMeta()
			if err := saveToConfig(srcFsMeta, meta); err != nil {
				return nil
			}
		} else {
			return nil
		}
	}

	return &opfsHdfsFs {
		meta: meta,
		Mutex: &sync.Mutex{},
	}
}

func (fs *opfsHdfsFs) GetMode() string {
	fs.Lock()
	defer fs.Unlock()
	return fs.meta.Mode
}

func (fs *opfsHdfsFs) SetMode(mode string) error{
	srcFsMeta := path.Join(hdfsSysDir, hdfsFsMeta)
	fs.Lock()
	defer fs.Unlock()
	if fs.meta.Mode == mode {
		return nil
	}
	fs.meta.Mode = mode
	if err := saveToConfig(srcFsMeta, &fs.meta); err != nil {
		return err
	}

	return nil
}

func (fs *opfsHdfsFs) GetRestoreFailedStorage() string {
	fs.Lock()
	defer fs.Unlock()
	return fs.meta.RestoreFailedStorage
}

func (fs *opfsHdfsFs) SetRestoreFailedStorage(value string) error{
	srcFsMeta := path.Join(hdfsSysDir, hdfsFsMeta)
	fs.Lock()
	defer fs.Unlock()
	if fs.meta.RestoreFailedStorage == value {
		return nil
	}
	fs.meta.RestoreFailedStorage = value
	if err := saveToConfig(srcFsMeta, &fs.meta); err != nil {
		return err
	}

	return nil
}

func (fs *opfsHdfsFs) SetAllowSnapshot(value bool) error {
	srcFsMeta := path.Join(hdfsSysDir, hdfsFsMeta)
	fs.Lock()
	defer fs.Unlock()
	if fs.meta.AllowSnapshot == value {
		return nil
	}
	fs.meta.AllowSnapshot = value
	if err := saveToConfig(srcFsMeta, &fs.meta); err != nil {
		return err
	}

	return nil
}

func (fs *opfsHdfsFs) GetAllowSnapshot() bool {
	fs.Lock()
	defer fs.Unlock()

	return fs.meta.AllowSnapshot
}

func opfsSetSafeMode(r *hdfs.SetSafeModeRequestProto) (*hdfs.SetSafeModeResponseProto, error) {
	action := r.GetAction()
	check := r.GetChecked()

	log.Printf("action %v, check %v", action, check)

	mode := modeNormal
	switch action.String() {
	case hdfs.SafeModeActionProto_SAFEMODE_LEAVE.String():
		globalFs.SetMode(modeNormal)
		mode = globalFs.GetMode()
	case hdfs.SafeModeActionProto_SAFEMODE_ENTER.String():
		globalFs.SetMode(modeSafe)
		mode = globalFs.GetMode()
	case hdfs.SafeModeActionProto_SAFEMODE_GET.String():
		mode = globalFs.GetMode()
	case hdfs.SafeModeActionProto_SAFEMODE_FORCE_EXIT.String():
		globalFs.SetMode(modeNormal)
		mode = globalFs.GetMode()

	default:
		panic(fmt.Errorf("not support action %v", action.String()))
	}

	res := &hdfs.SetSafeModeResponseProto {
		Result: proto.Bool(mode == modeSafe),
	}

	return res, nil
}


