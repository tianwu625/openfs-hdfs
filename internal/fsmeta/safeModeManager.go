package fsmeta

import (
	"errors"
	"context"
	"sync"

	"github.com/openfs/openfs-hdfs/internal/rpc"
	"github.com/openfs/openfs-hdfs/internal/logger"
	"google.golang.org/protobuf/proto"
)

const (
	DenyFsWrite = (1 << iota)
	DenyFsRead
	DenyFsManage
	DenyAll = (DenyFsWrite | DenyFsRead | DenyFsManage)
)

type SafeModeManager struct {
	sync.RWMutex
	fs *OpfsHdfsFs
	level int
	fsWrite rpc.RpcMap
	fsRead rpc.RpcMap
	fsManage rpc.RpcMap
}

func NewSafeModeManager(fs *OpfsHdfsFs, fsWrite rpc.RpcMap, fsRead rpc.RpcMap, fsManage rpc.RpcMap) *SafeModeManager {
	return &SafeModeManager {
		fs: fs,
		fsWrite: fsWrite,
		fsRead: fsRead,
		fsManage: fsManage,
	}
}

var ErrInSafeMode error = errors.New("file system in safe mode")

func (smm *SafeModeManager) ProcessBefore(client *rpc.RpcClient, ctx context.Context, r proto.Message) error {
	smm.RLock()
	defer smm.RUnlock()
	mode := smm.fs.GetMode()
	if mode == ModeSafe {
		method := logger.GetReqMethod(ctx)
		if smm.level & DenyFsWrite != 0 &&
		   smm.fsWrite.InKeys(method) {
			   return ErrInSafeMode
		}
		if smm.level & DenyFsRead != 0 &&
		   smm.fsRead.InKeys(method) {
			   return ErrInSafeMode
		}
		if smm.level & DenyFsManage != 0 &&
		   smm.fsManage.InKeys(method) {
			   return ErrInSafeMode
		}
	}

	return nil
}

func (smm *SafeModeManager) SetMode(mode string, level int) error {
	smm.Lock()
	defer smm.Unlock()
	if mode == ModeSafe {
		smm.level = level
	} else {
		smm.level = 0
	}

	return smm.fs.SetMode(mode)
}

func (smm *SafeModeManager) GetMode() string {
	smm.RLock()
	defer smm.RUnlock()
	return smm.fs.GetMode()
}

var globalSafeModeManager *SafeModeManager

func InitSafeModeManager(fs *OpfsHdfsFs, fsWrite rpc.RpcMap, fsRead rpc.RpcMap, fsManage rpc.RpcMap) *SafeModeManager {
	globalSafeModeManager = NewSafeModeManager(fs, fsWrite, fsRead, fsManage)
	return globalSafeModeManager
}

func GetGlobalSafeModeManager() *SafeModeManager {
	return globalSafeModeManager
}
