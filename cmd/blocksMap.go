package cmd

import (
	"sync"
	"syscall"

	hconf "github.com/openfs/openfs-hdfs/hadoopconf"
	"github.com/openfs/openfs-hdfs/internal/opfsBlocksMap"
)

type blocksMap struct {
	sync.RWMutex
	opfsBlocksMap.BlocksInterface
	blocks map[string]*opfsBlocksMap.Blocks //key is filename
}

func(bsm *blocksMap) AppendBlock (filename string, prev *opfsBlocksMap.Blockmap, excludes []string) (*opfsBlocksMap.Blockmap, error) {
	bsm.Lock()
	defer bsm.Unlock()
	bm, err := bsm.AddBlock(filename,
		opfsBlocksMap.AddBlockOptWithPrev(prev),
		opfsBlocksMap.AddBlockOptWithExcludes(excludes),
		)
	if err != nil {
		return nil, err
	}
	return bm, nil
}

func(bsm *blocksMap) CancleBlock(filename string, bm *opfsBlocksMap.Blockmap) error {
	bsm.Lock()
	defer bsm.Unlock()
	err := bsm.CancleBlock(filename, bm)
	if err != nil {
		return err
	}
	return nil
}

func (bsm *blocksMap) CommitBlock(filename string, bm *opfsBlocksMap.Blockmap) error {
	bsm.Lock()
	defer bsm.Unlock()
	err := bsm.CommitBlock(filename, bm)
	if err != nil {
		return err
	}

	return nil
}

func (bsm *blocksMap) DeleteBlocks(filename string) error {
	bsm.Lock()
	defer bsm.Unlock()
	delete(bsm.blocks, filename)
	return bsm.Delete(filename)
}

func(bsm *blocksMap) LoadFileAllBlocks(filename string) error {
	bsm.Lock()
	defer bsm.Unlock()
	bs, err := bsm.Load(filename)
	if err != nil {
		return err
	}
	bsm.blocks[filename] = bs

	return nil
}

func(bsm *blocksMap) GetFileBlock(filename string, offset uint64) (*opfsBlocksMap.Blockmap, error) {
	bsm.RLock()
	defer bsm.RUnlock()

	bs, ok := bsm.blocks[filename]
	if !ok {
		return nil, syscall.ENOENT
	}
	if !bsm.Valid(filename, bs.Update) {
		return nil, syscall.EINVAL
	}
	index := bs.GetOffIndex(offset)
	if index >= uint64(len(bs.Bms)) {
		return nil, syscall.ENOENT
	}
	bm := bs.Bms[index]
	locs := make([]*opfsBlocksMap.BlockLoc, len(bm.L))
	for _, l := range bm.L {
		locs = append(locs, &opfsBlocksMap.BlockLoc{
			DatanodeUuid: l.DatanodeUuid,
			StorageUuid: l.StorageUuid,
		})
	}
	return &opfsBlocksMap.Blockmap {
		OffStart:bm.OffStart,
		OffEnd:bm.OffEnd,
		B: &opfsBlocksMap.Block {
			PoolId: bm.B.PoolId,
			BlockId: bm.B.BlockId,
			Generation: bm.B.Generation,
			Num: bm.B.Num,
		},
		L: locs,
	}, nil
}

func(bsm *blocksMap) GetOffIndex(filename string, off uint64) (int, error) {
	bsm.RLock()
	defer bsm.RUnlock()
	bs, ok := bsm.blocks[filename]
	if !ok {
		return 0, syscall.ENOENT
	}
	index := bs.GetOffIndex(off)
	return int(index), nil
}

func(bsm *blocksMap) GetFileAllBlocks(filename string) ([]*opfsBlocksMap.Blockmap, error) {
	bmRes := make([]*opfsBlocksMap.Blockmap, 0)
	bsm.RLock()
	defer bsm.RUnlock()

	bs, ok := bsm.blocks[filename]
	if !ok {
		return bmRes, syscall.ENOENT
	}
	if !bsm.Valid(filename, bs.Update) {
		return bmRes, syscall.EINVAL
	}
	for _, bm := range bs.Bms {
		locs := make([]*opfsBlocksMap.BlockLoc, 0, len(bm.L))
		for _, l := range bm.L {
			locs = append(locs, &opfsBlocksMap.BlockLoc{
				DatanodeUuid: l.DatanodeUuid,
				StorageUuid: l.StorageUuid,
			})
		}
		bmRes = append(bmRes, &opfsBlocksMap.Blockmap{
			OffStart: bm.OffStart,
			OffEnd: bm.OffEnd,
			B: &opfsBlocksMap.Block {
				PoolId: bm.B.PoolId,
				BlockId: bm.B.BlockId,
				Generation: bm.B.Generation,
				Num: bm.B.Num,
			},
			L: locs,
		})
	}

	return bmRes, nil
}
func (bsm *blocksMap) CreateBlocksMap (filename string, replicate int, blockSize uint64) error {
	return bsm.Create(filename, replicate, blockSize)
}

func (bsm *blocksMap) RenameBlocksMap(src, dst string) error {
	return bsm.Rename(src, dst)
}

func (bsm *blocksMap) CompleteFile(src, client string, blast *opfsBlocksMap.Blockmap, fid uint64) error {
	bsm.Lock()
	defer bsm.Unlock()
	bs := bsm.blocks[src]
	return bsm.Complete(src, blast, bs)
}

func NewBlocksMap(i opfsBlocksMap.BlocksInterface) *blocksMap {
	return &blocksMap{
		blocks: make(map[string]*opfsBlocksMap.Blocks),
		BlocksInterface: i,
	}
}

var globalBlocksMap *blocksMap

func initBlocksMap(core hconf.HadoopConf) error {
	if err := opfsBlocksMap.InitOpfsBlocksMap(core); err != nil {
		return err
	}
	obm := opfsBlocksMap.GetOpfsBlocksMap()

	globalBlocksMap = NewBlocksMap(obm)

	return nil
}

func getBlocksMap() *blocksMap {
	return globalBlocksMap
}
