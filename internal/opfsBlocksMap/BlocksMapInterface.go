package opfsBlocksMap

import (
	"time"
	"log"
	"strings"
	"fmt"
)

type Block struct {
	PoolId string
	BlockId uint64
	Generation uint64
	Num uint64
}

type BlockLoc struct {
	DatanodeUuid string
	StorageUuid string
}

type Blockmap struct {
	OffStart uint64
	OffEnd uint64
	B *Block
	L []*BlockLoc
}
func (bm *Blockmap) String() string {
	head := fmt.Sprintf("start %v, end %v", bm.OffStart, bm.OffEnd)
	id := fmt.Sprintf("id: poolid %v, blockid %v, gen %v, num %v",bm.B.PoolId, bm.B.BlockId, bm.B.Generation, bm.B.Num)

	locs := make([]string, 0, len(bm.L))
	for _, loc := range bm.L {
		locs = append(locs, fmt.Sprintf("datanodeuuid %v, storageuuid %v", loc.DatanodeUuid, loc.StorageUuid))
	}
	locsum := strings.Join(locs, "//")

	return  head + "\n" + id + "\n" + locsum
}

type Blocks struct {
	Filename string
	BlockSize uint64
	Update time.Time
	Bms []*Blockmap
}

func (bs *Blocks) GetOffIndex(off uint64) uint64 {
	log.Printf("BlockSize %v off %v", bs.BlockSize, off)
	return off/bs.BlockSize
}

type AddBlockOptions struct {
	Prev *Blockmap
	Excludes []string
}

type AddBlockOpt func(*AddBlockOptions)

func AddBlockOptWithPrev(prev *Blockmap) AddBlockOpt {
	return func(opt *AddBlockOptions) {
		opt.Prev = prev
	}
}

func AddBlockOptWithExcludes(excludes []string) AddBlockOpt {
	return func(opt *AddBlockOptions) {
		opt.Excludes = excludes
	}
}

type BlocksInterface interface {
	Load(filename string)(*Blocks, error)
	Save(filename string,bs *Blocks)error
	AddBlock(filename string, opts ...AddBlockOpt) (*Blockmap, error)
	CancleBlock(filename string, bm *Blockmap) error
	CommitBlock(filename string, bm *Blockmap) error
	Create(filename string, replicate int, blockSize uint64) error
	Rename(src, dst string) error
	Valid(filename string, update time.Time) bool
	Complete(filename string, last *Blockmap, bs *Blocks) error
	Delete(filename string) error
}
