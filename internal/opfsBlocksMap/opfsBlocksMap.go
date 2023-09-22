package opfsBlocksMap

import (
	"sync"
	"path"
	"time"
	"errors"
	"log"
	"os"
	"fmt"
	"io"
	"bytes"
	"syscall"
	"strings"

	"github.com/openfs/openfs-hdfs/internal/logger"
	"github.com/openfs/openfs-hdfs/internal/opfs"
	"github.com/openfs/openfs-hdfs/internal/datanodeMap"
	hconf "github.com/openfs/openfs-hdfs/hadoopconf"
	jsoniter "github.com/json-iterator/go"
	"github.com/google/uuid"
)

type opfsBlockId struct {
	PoolId string `json:"-"`
	BlockId uint64 `json:"id, omitempty"`
	Generation uint64 `json:"gen, omitempty"`
	Num uint64 `json:"num, omitempty"`
}

type opfsBlockLoc struct {
	State int `json: "state, omitempty"`
	Size uint64 `json: "size, omitempty"`
	DatanodeUuid string `json:"datanode, omitempty"`
	StorageUuid string `json:"storage, omitempty"`
}

func (bl *opfsBlockLoc) equal(l *opfsBlockLoc) bool {
	if bl.StorageUuid != l.StorageUuid ||
	   bl.DatanodeUuid != l.DatanodeUuid {
		return false
	}

	return true
}


const (
	stateBlockInit = iota
	stateBlockConstruct
	stateBlockCommit
	stateBlockCancle
)

type opfsBlock struct {
	State int `json:"state, omitempty"`
	Start uint64 `json:"offStart, omitempty"`
	End uint64 `json:"offEnd, omitempty"`
	Id *opfsBlockId `json:"bid, omitempty"`
	Locs []*opfsBlockLoc `json:"blocs, omitempty"`
}

func (ob *opfsBlock) CloneLocs() []*opfsBlockLoc {
	res := make([]*opfsBlockLoc, 0, len(ob.Locs))
	for _, l := range ob.Locs {
		lc := &opfsBlockLoc {
			DatanodeUuid: l.DatanodeUuid,
			StorageUuid: l.StorageUuid,
		}
		res = append(res, lc)
	}

	return res
}

func (ob *opfsBlock) CloneId() *opfsBlockId {
	return &opfsBlockId {
		PoolId: ob.Id.PoolId,
		BlockId: ob.Id.BlockId,
		Generation: ob.Id.Generation,
		Num: ob.Id.Num,
	}
}

func (ob *opfsBlock) Clone() *opfsBlock {
	return &opfsBlock {
		State: ob.State,
		Start: ob.Start,
		End: ob.End,
		Id: ob.CloneId(),
		Locs: ob.CloneLocs(),
	}
}

func (ob *opfsBlock) String() string {
	head := fmt.Sprintf("state %v, start %v, end %v",
				ob.State, ob.Start, ob.End)
	id := fmt.Sprintf("id: poolid %v, blockid %v, gen %v, num %v",ob.Id.PoolId, ob.Id.BlockId, ob.Id.Generation, ob.Id.Num)

	locs := make([]string, 0, len(ob.Locs))
	for _, loc := range ob.Locs {
		locs = append(locs, fmt.Sprintf("state %v, size %v, datanodeuuid %v, storageuuid %v", loc.State,
	                      loc.Size, loc.DatanodeUuid, loc.StorageUuid))
	}
	locsum := strings.Join(locs, "//")

	return  head + "\n" + id + "\n" + locsum
}

var ErrNoLocs error = errors.New("locs number not enough")

func (ob *opfsBlock) excludeDatanode(uuids []string, replicate int) error {
	if len(uuids) == 0 {
		return nil
	}
outer:
	for {
		for i, l := range ob.Locs {
			for _, uuid := range uuids {
				if uuid == l.DatanodeUuid {
					ob.Locs = append(ob.Locs[:i], ob.Locs[i+1:]...)
					continue outer
				}
			}
		}
		break
	}

	if len(ob.Locs) != replicate {
		return ErrNoLocs
	}

	return nil
}

func (ob *opfsBlock) Equal(b *opfsBlock) bool {
	if ob.Id.PoolId != b.Id.PoolId {
		return false
	}

	if ob.Id.BlockId != b.Id.BlockId {
		return false
	}

	if ob.Id.Generation != b.Id.Generation {
		return false
	}

	return true
}

func (ob *opfsBlock) getCommitCount() int {
	res := 0
	for _, loc := range ob.Locs {
		if loc.State == stateBlockCommit {
			res++
		}
	}

	return res
}

func (ob *opfsBlock) getCommitLocs() []*opfsBlockLoc {
	res := make([]*opfsBlockLoc, 0, len(ob.Locs))
	for _, loc := range ob.Locs {
		if loc.State == stateBlockCommit {
			res = append(res, loc)
		}
	}

	return res
}

func (ob *opfsBlock) getLocByLoc(l *opfsBlockLoc) *opfsBlockLoc {
	for _, loc := range ob.Locs {
		if loc.equal(l) {
			return loc
		}
	}

	return nil
}

func (ob *opfsBlock) commitLocs(locs []*opfsBlockLoc, size uint64) error {
	for _, loc := range locs {
		loct := ob.getLocByLoc(loc)
		if loct == nil {
			return syscall.ENOENT
		}
		loct.State = stateBlockCommit
		loct.Size = size
	}

	return nil
}


type opfsBlocks struct {
	Filename string `json:"-"`
	BlockSize uint64 `json:"blocksize, omitempty"`
	Replicate int `json:"replicate, omitempty"`
	ReplicateMin int `json:"replicatemin, omitempty"`
	Blocks []*opfsBlock `json:"blocks, omitempty"`
}

type opfsBlocksMap struct {
	sync.RWMutex
	conf *opfsBlocksMapConf
	blocksmap map[string]*opfsBlocks
	constructmap map[string]*opfsBlocks
}
const (
	hdfsSysDir = "/.hdfs.sys"
	hdfsBlocksDir = hdfsSysDir + "/blocksmap"
	hdfsBlocksFile = "blocksMeta"
	tmpdir = "tmp"
)

func lastTime(ts ...time.Time) time.Time {
	res := time.Unix(0, 0)
	for _, t := range ts {
		if t.Equal(time.Unix(0, 0)) {
			continue
		}
		if t.After(res) {
			res = t
		}
	}

	return res
}

var ErrGetModifyTimeFail error = errors.New("get modify time fail")

func opfsGetModifyTime(src string) (time.Time, error) {
	f, err := opfs.Open(src)
	if err != nil {
		return time.Time{}, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return time.Time{}, err
	}

	return fi.ModTime(), nil
}

func opfsBlockGetUpdateTime(src string) (time.Time, error) {
	configPath := path.Join(hdfsBlocksDir, src, hdfsBlocksFile)
	t1, err := opfsGetModifyTime(configPath)
	if err != nil {
		t1 = time.Unix(0,0)
	}
	t2, err := opfsGetModifyTime(src)
	if err != nil {
		t2 = time.Unix(0,0)
	}
	t := lastTime(t1, t2)
	if t.Equal(time.Unix(0, 0)) {
		return t, ErrGetModifyTimeFail
	}
	return t, nil
}

func convertLocsToBlockLoc(locs []*opfsBlockLoc) []*BlockLoc {
	res := make([]*BlockLoc, 0, len(locs))
	for _, loc := range locs {
		datanodeuuid := loc.DatanodeUuid
		storageuuid := loc.StorageUuid
		if loc.DatanodeUuid == datanodeMap.LocLocation &&
		   loc.StorageUuid == datanodeMap.LocLocation {
			dm := datanodeMap.GetGlobalDatanodeMap()
			datanodeuuid = dm.GetLocDatanodeUuid()
			storageuuid = dm.GetStorageUuid(datanodeuuid)
		}
		bl := &BlockLoc {
			DatanodeUuid: datanodeuuid,
			StorageUuid: storageuuid,
		}
		res = append(res, bl)
	}

	return res
}

func convertToBlocks(bs *opfsBlocks) *Blocks {
	t, err := opfsBlockGetUpdateTime(bs.Filename)
	if err != nil {
		return nil
	}
	res := &Blocks {
		Filename:bs.Filename,
		BlockSize: bs.BlockSize,
		Update: t,
		Bms: make([]*Blockmap, 0, len(bs.Blocks)),
	}
	for _, b := range bs.Blocks {
		if b.State != stateBlockCommit {
			log.Printf("bs %v, opfsBlock %v", bs, b)
			continue
		}
		bm := &Blockmap {
			OffStart: b.Start,
			OffEnd: b.End,
			B: &Block {
				PoolId:b.Id.PoolId,
				BlockId:b.Id.BlockId,
				Generation:b.Id.Generation,
				Num:b.Id.Num,
			},
			L: convertLocsToBlockLoc(b.Locs),
		}
		res.Bms = append(res.Bms, bm)
	}

	return res
}

func loadFromFileToBlocks(filename string, blocksize, start, end uint64, replicate, replicatemin int) (*opfsBlocks, error) {
	count := ((start + end - start + 1) / blocksize - start / blocksize) + 1
	bs := make([]*opfsBlock, 0, int(count))
	dm := datanodeMap.GetGlobalDatanodeMap()
	for i := uint64(0); i < count; i++ {
		b := &opfsBlock {
			State: stateBlockCommit,
			Start: start + i * blocksize,
			End: start + (i+1) * blocksize  - 1,
			Id: &opfsBlockId {
				PoolId: filename,
				BlockId: start + i * blocksize,
				Generation: 0,
				Num: blocksize,
			},
			Locs: []*opfsBlockLoc {
				&opfsBlockLoc {
					DatanodeUuid: dm.GetDatanodeUuid(),
				},
			},
		}
		b.Locs[0].StorageUuid = dm.GetStorageUuid(b.Locs[0].DatanodeUuid)
		if i + 1 == count {
			b.Id.Num = end - i * blocksize + 1
		}
		log.Printf("b get %v", b)
		bs = append(bs, b)
	}
	return &opfsBlocks {
		Filename: filename,
		BlockSize: blocksize,
		Replicate: replicate,
		ReplicateMin: replicatemin,
		Blocks: bs,
	}, nil
}

func getFileSize(src string) (int64, error) {
	f, err := opfs.Open(src)
	if err != nil {
		return 0, nil
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, nil
	}
	return fi.Size(), nil
}

func mergeBlockFromFile(bs *opfsBlocks) *opfsBlocks {
	configPath := path.Join(hdfsBlocksDir, bs.Filename, hdfsBlocksFile)
	t1, err := opfsGetModifyTime(configPath)
	if err != nil {
		panic(err)
	}
	t2, err := opfsGetModifyTime(bs.Filename)
	if err != nil {
		panic(err)
	}
	if t2.After(t1) {
		off := uint64(0)
		if len(bs.Blocks) != 0 {
			off = bs.Blocks[len(bs.Blocks) - 1].End + 1
		}
		size, _ := getFileSize(bs.Filename)
		end := uint64(size - 1)
		if end < off {
			var err error
			bs, err = loadFromFileToBlocks(bs.Filename, bs.BlockSize, 0, end, bs.Replicate, bs.ReplicateMin)
			if err != nil {
				return nil
			}
		} else {
			abs, err := loadFromFileToBlocks(bs.Filename, bs.BlockSize, off, end, bs.Replicate, bs.ReplicateMin)
			if err != nil {
				return nil
			}
			bs.Blocks = append(bs.Blocks, abs.Blocks...)
		}
	}

	return bs
}

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
//	log.Printf("src %v", src)
	b, err := opfsReadAll(src)
	if err != nil {
		return err
	}
//	log.Printf("len b %v", len(b))
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	if err = json.Unmarshal(b, object); err != nil {
		log.Printf("unmarshal fail %v, %T", err, object)
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

const (
	defaultConfigPerm = 0777
)

func saveToConfig(src string, object interface{}) error {
	//logger.LogIf(nil, fmt.Errorf("object %T, src %v", object, src))
	if blocks, ok := object.(*opfsBlocks); ok {
		log.Printf("save contect %v", blocks)
	}
	srcTmp := path.Join(hdfsSysDir, tmpdir, GetRandomFileName())

	err := opfs.MakeDirAll(path.Dir(srcTmp), os.FileMode(defaultConfigPerm))
	if err != nil {
		return err
	}
	f, err := opfs.OpenWithCreate(srcTmp, os.O_WRONLY | os.O_CREATE, os.FileMode(defaultConfigPerm))
	if err != nil {
		return err
	}
	defer f.Close()
	defer func(src string) {
		opfs.RemoveFile(src)
	}(srcTmp)
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

func fillinPoolIdWithFilename(blocks *opfsBlocks) {
	if blocks.Filename == "" {
		panic(fmt.Errorf("json will overwrite all struct %v", blocks))
	}

	for _, b := range blocks.Blocks {
		b.Id.PoolId = blocks.Filename
	}
}

func (obm *opfsBlocksMap) loadWithoutLock(filename string) (*Blocks, error) {
	blocks, ok := obm.blocksmap[filename]
	if !ok {
		configPath := path.Join(hdfsBlocksDir, filename, hdfsBlocksFile)
		blocks = &opfsBlocks {
			Filename: filename,
		}
		if err := loadFromConfig(configPath, blocks); err != nil {
			if os.IsNotExist(err) {
				size, _ := getFileSize(filename)
				end := uint64(size - 1)
				blocks, err = loadFromFileToBlocks(filename, obm.conf.defaultBlockSize, 0, end,
								   obm.conf.defaultReplicate, obm.conf.defaultReplicateMin)
				if err != nil {
					logger.LogIf(nil, fmt.Errorf("load from opfs file %v, %v", filename, err))
					return nil, err
				}
				obm.blocksmap[filename] = blocks
				return convertToBlocks(blocks), nil
			}
			return nil, err
		}
		fillinPoolIdWithFilename(blocks)
		//process filename change from openfs fs interface
		blocks = mergeBlockFromFile(blocks)
		obm.blocksmap[filename] = blocks
		return convertToBlocks(blocks), nil
	}
	return convertToBlocks(blocks), nil
}

func (obm *opfsBlocksMap) Load (filename string) (*Blocks, error) {
	obm.Lock()
	defer obm.Unlock()
	return obm.loadWithoutLock(filename)
}

func convertBlocksToOpfs(bs *Blocks) *opfsBlocks {
	obs := &opfsBlocks {
		Filename: bs.Filename,
		BlockSize: bs.BlockSize,
		Blocks: make([]*opfsBlock, 0, len(bs.Bms)),
	}

	for _, bm := range bs.Bms {
		ob := &opfsBlock {
			State:stateBlockCommit,
			Start:bm.OffStart,
			End:bm.OffEnd,
			Id: &opfsBlockId {
				PoolId: bm.B.PoolId,
				BlockId: bm.B.BlockId,
				Generation: bm.B.Generation,
				Num: bm.B.Num,
			},
			Locs: make([]*opfsBlockLoc, 0, len(bm.L)),
		}
		for _, loc := range bm.L {
			ol := &opfsBlockLoc {
				DatanodeUuid: loc.DatanodeUuid,
				StorageUuid: loc.StorageUuid,
			}
			ob.Locs = append(ob.Locs, ol)
		}
		obs.Blocks = append(obs.Blocks, ob)
	}

	return obs
}

func (obm *opfsBlocksMap)Save(filename string, bs *Blocks) error {
	obm.RLock()
	defer obm.RUnlock()
	configPath := path.Join(hdfsBlocksDir, filename, hdfsBlocksFile)
	blocks, ok := obm.blocksmap[filename]
	if !ok {
		obs := convertBlocksToOpfs(bs)
		return saveToConfig(configPath, obs)
	}
	return saveToConfig(configPath, blocks)
}

func convertToBlockmap(b *opfsBlock) *Blockmap {
	return &Blockmap {
		OffStart: b.Start,
		OffEnd: b.End,
		B: &Block {
			PoolId: b.Id.PoolId,
			BlockId: b.Id.BlockId,
			Generation: b.Id.Generation,
			Num: b.Id.Num,
		},
		L: convertLocsToBlockLoc(b.Locs),
	}
}

func getOpfsBlockLocFromDatanodeLoc(dlocs []*datanodeMap.DatanodeLoc) []*opfsBlockLoc {
	res := make([]*opfsBlockLoc, 0, len(dlocs))
	for _, l := range dlocs {
		bl := &opfsBlockLoc {
			DatanodeUuid: l.Node.Id.Uuid,
			StorageUuid: l.Storage.Uuid,
		}
		res = append(res, bl)
	}

	return res
}

func newOpfsBlock(prev *opfsBlock, bs *opfsBlocks, excludes []string) (*opfsBlock, error) {
	start := uint64(0)
	end := bs.BlockSize - 1
	bid := uint64(0)
	if prev != nil {
		start = prev.End + 1
		end = start + bs.BlockSize - 1
		bid = prev.Id.BlockId + bs.BlockSize
	}
	dm := datanodeMap.GetGlobalDatanodeMap()
	dlocs, err := dm.GetDatanodeWithAllocMethod(datanodeMap.AllocMethodOptions {
		Method: datanodeMap.OpfsEcMethod,
		Replicate: bs.Replicate,
		ReplicateMin: bs.ReplicateMin,
		StorageType: "DISK",
		Excludes: excludes,
	})
	if err != nil {
		logger.LogIf(nil, fmt.Errorf("alloc dlocs fail %v", err))
		return nil, err
	}
	locs := getOpfsBlockLocFromDatanodeLoc(dlocs)
	return &opfsBlock {
		State: stateBlockConstruct,
		Start: start,
		End: end,
		Id: &opfsBlockId {
			PoolId: bs.Filename,
			BlockId: bid,
			Generation: 0,
			Num: 0,
		},
		Locs: locs,
	}, nil
}

func (obm *opfsBlocksMap) addConstructmap(filename string, ob *opfsBlock) error {
	ob.State = stateBlockConstruct
	bs, ok := obm.constructmap[filename]
	if !ok {
		obm.constructmap[filename] = &opfsBlocks {
			Filename: filename,
			BlockSize: ob.End - ob.Start + 1,
			Blocks: make([]*opfsBlock, 0),
		}
		bs = obm.constructmap[filename]
	}
	bs.Blocks = append(bs.Blocks, ob)

	return nil
}

func (obm *opfsBlocksMap) getConstructBlock(filename string, bt *opfsBlock) *opfsBlock{
	bs, ok :=  obm.constructmap[filename]
	if !ok {
		panic(fmt.Errorf("filename construct blocks in cache"))
	}
	for _, b := range bs.Blocks {
		if b.Equal(bt) {
			return b
		}
	}
	return nil
}

func opfsUnlockFile(filename string) error {
	fmt.Printf("unlock file %v\n", filename)
	return nil
}

func deferUnlock(filename string, err *error) {
	if *err != nil {
		globalLocks.Lock()
		defer globalLocks.Unlock()
		_, ok := globalLocks.locks[filename]
		if !ok {
			return
		}
		opfsUnlockFile(filename)
		delete(globalLocks.locks, filename)
	}

	return
}

type opfsFileLock struct {
	LockId uint64
}

type locksMap struct {
	sync.RWMutex
	locks map[string]*opfsFileLock
}

var globalLocks *locksMap

func opfsLockFile(filename string) *opfsFileLock {
	fmt.Printf("lock file %v\n", filename)
	return &opfsFileLock {
		LockId: func(string) uint64 {
			sum := 0
			for _, i := range []byte(filename) {
				sum += int(i)
			}
			return uint64(sum)
		}(filename),
	}
}

var ErrLockOpfsFileFail error = errors.New("opfs lock file fail")

func opfsLocks(filename string) error {
	globalLocks.RLock()
	l, ok := globalLocks.locks[filename]
	if !ok {
		globalLocks.RUnlock()
		globalLocks.Lock()
		l = opfsLockFile(filename)
		if l == nil {
			globalLocks.Unlock()
			return ErrLockOpfsFileFail
		}
		globalLocks.locks[filename] = l
		globalLocks.Unlock()
		return nil
	}
	globalLocks.RUnlock()
	return nil
}


func (obm *opfsBlocksMap) AddBlock(filename string, opts ...AddBlockOpt) (resB *Blockmap, err error) {
	options := &AddBlockOptions{
		Excludes: make([]string, 0),
	}
	for _, opt := range opts {
		opt(options)
	}
	obm.Lock()
	defer obm.Unlock()
	err = opfsLocks(filename)
	if err != nil {
		return nil, err
	}
	defer deferUnlock(filename, &err)
	if options.Prev != nil {
		bt := convertToOpfsBlock(options.Prev)
		blocks, ok := obm.blocksmap[filename]
		if !ok {
			panic(fmt.Errorf("filename %v should in both blocks and constructs", filename))
		}
		b := obm.getConstructBlock(filename, bt)
		b.Id.Num = bt.Id.Num
		if bt.Id.Num != b.End - b.Start + 1 {
			panic(fmt.Errorf("append file? will do this case"))
		}
		bn, err := newOpfsBlock(b, blocks, options.Excludes)
		if err != nil {
			return nil, err
		}
		err = obm.addConstructmap(filename, bn)
		if err != nil {
			return nil, err
		}
		return convertToBlockmap(bn), nil
	}
	blocks, ok := obm.blocksmap[filename]
	if !ok {
		obm.loadWithoutLock(filename)
		blocks = obm.blocksmap[filename]
	}
	var lastb *opfsBlock
	if len(blocks.Blocks) != 0 {
		b := blocks.Blocks[len(blocks.Blocks) - 1]
		if b.Id.Num < blocks.BlockSize {
			bc := b.Clone()
			err = bc.excludeDatanode(options.Excludes, blocks.Replicate)
			if err != nil {
				return nil, err
			}
			err = obm.addConstructmap(filename, bc)
			if err != nil {
				return nil, err
			}
			return convertToBlockmap(bc), nil
		}
		lastb = b
	}
	bn, err := newOpfsBlock(lastb, blocks, options.Excludes)
	if err != nil {
		return nil, err
	}
	log.Printf("bn %v", bn)
	err = obm.addConstructmap(filename, bn)
	if err != nil {
		return nil, err
	}

	return convertToBlockmap(bn), nil
}

func convertOpfsBlockLoc(locs []*BlockLoc) []*opfsBlockLoc {
	res := make([]*opfsBlockLoc, 0, len(locs))
	for _, loc := range locs {
		r := &opfsBlockLoc {
			DatanodeUuid: loc.DatanodeUuid,
			StorageUuid: loc.StorageUuid,
		}
		res = append(res, r)
	}

	return res
}

func convertToOpfsBlock(bm *Blockmap) *opfsBlock {
	if bm == nil {
		return nil
	}
	return &opfsBlock {
		Start: bm.OffStart,
		End: bm.OffEnd,
		Id: &opfsBlockId {
			PoolId: bm.B.PoolId,
			BlockId: bm.B.BlockId,
			Generation: bm.B.Generation,
			Num: bm.B.Num,
		},
		Locs: convertOpfsBlockLoc(bm.L),
	}
}

func (obm *opfsBlocksMap) CancleBlock(filename string, bm *Blockmap) error {
	bt := convertToOpfsBlock(bm)
	obm.Lock()
	defer obm.Unlock()
	bs, ok := obm.constructmap[filename]
	if !ok {
		panic(fmt.Errorf("cancel the block not not in construct? %v", obm))
	}
	for i, b := range bs.Blocks {
		if b.Equal(bt) {
			bs.Blocks = append(bs.Blocks[:i], bs.Blocks[i+1:]...)
			break
		}
	}

	return nil
}

func (obm *opfsBlocksMap) CommitBlock(filename string, bm *Blockmap) error {
	bt := convertToOpfsBlock(bm)
	obm.Lock()
	defer obm.Unlock()
	bs, ok := obm.constructmap[filename]
	if !ok {
		panic(fmt.Errorf("commit %v the block not not in construct? %v", filename, obm))
	}
	var ba *opfsBlock
	for _, b := range bs.Blocks {
		if !b.Equal(bt) {
			continue
		}
		ba = b
		ba.Id.Num = bt.Id.Num
		ba.commitLocs(bt.Locs, bt.Id.Num)
		if ba.getCommitCount() >= bs.ReplicateMin {
			ba.State = stateBlockCommit
		}
		break
	}
	if ba == nil {
		panic(fmt.Errorf("only construct block can commit %v", obm))
	}

	return nil
}

func (obm *opfsBlocksMap) Create(filename string, replicate int, blockSize uint64) error {
	obm.Lock()
	defer obm.Unlock()
	if replicate == 0 {
		logger.LogIf(nil, fmt.Errorf("replicate is not set with default %v instead", obm.conf.defaultReplicate))
		replicate = obm.conf.defaultReplicate
	}
	if blockSize == 0 {
		logger.LogIf(nil, fmt.Errorf("replicate is not set with default %v instead", obm.conf.defaultReplicate))
		blockSize = obm.conf.defaultBlockSize
	}
	replicatemin := obm.conf.defaultReplicateMin

	bs := &opfsBlocks {
		Filename:filename,
		BlockSize: blockSize,
		Replicate: replicate,
		ReplicateMin: replicatemin,
	}
	obm.blocksmap[filename] = bs
	configPath := path.Join(hdfsBlocksDir, filename, hdfsBlocksFile)
	log.Printf("save config %v", configPath)
	return saveToConfig(configPath, bs)
}

func isOpfsDir(src string) (bool, error) {
	f, err := opfs.Open(src)
	if err != nil {
		return false, err
	}

	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return false, err
	}

	return fi.IsDir(), nil
}

// do blocks rename after namespace rename
func (obm *opfsBlocksMap) Rename(src, dst string) error {
	dstConfigPath := path.Join(hdfsBlocksDir, dst)
	srcConfigPath := path.Join(hdfsBlocksDir, src)
	if err := opfs.RenamePath(srcConfigPath, dstConfigPath, defaultConfigPerm); err != nil {
		if os.IsNotExist(err) {
			isDir, err2 := isOpfsDir(dst)
			if err2 != nil {
				panic(err2)
			}
			if !isDir {
				panic(fmt.Errorf("src %v, %v not exist", src, srcConfigPath))
			}
			logger.LogIf(nil, fmt.Errorf("src %v, %v not exist", src, srcConfigPath))
			return nil
		}
		logger.LogIf(nil, err)
		return err
	}

	return nil
}

func (obm *opfsBlocksMap) Valid(filename string, update time.Time) bool {
	t, err := opfsBlockGetUpdateTime(filename)
	if err != nil {
		return false
	}
	if t.After(update) {
		return false
	}

	return true
}

var ErrInvalidLast error = errors.New("last block mismatch with record in namenode")
var ErrInvalidSizeInBlock error = errors.New("size in block mismatch with reported by datanode")
var ErrNotCommited error = errors.New("block not in commit state")
var errUnlock error = errors.New("unlock file")

func (obm *opfsBlocksMap) Complete(filename string, last *Blockmap, blocks *Blocks) error {
	lastb := convertToOpfsBlock(last)
	obm.Lock()
	defer obm.Unlock()
	bs, ok := obm.blocksmap[filename]
	if !ok {
		panic(fmt.Errorf("complete file %v not in cache", filename))
	}
	cbs, ok := obm.constructmap[filename]
	if !ok {
		//this is a touhz file, size of file is zero
		return nil
		//panic(fmt.Errorf("complete file %v not in construct", filename))
	}
	for i, b := range cbs.Blocks {
		if b.State != stateBlockCommit {
			return ErrNotCommited
		}
		if i == len(cbs.Blocks) - 1 &&
		   lastb != nil {
	           if !b.Equal(lastb) {
			   return ErrInvalidLast
		   }
		   if b.Id.Num != lastb.Id.Num {
			   return ErrInvalidSizeInBlock
		   }
		}
	}
	delete(obm.constructmap, filename)
	appendBlocks := false
	var nowLast *opfsBlock
	if len(bs.Blocks) == 0 {
		nowLast = nil
	} else {
		nowLast = bs.Blocks[len(bs.Blocks) - 1]
	}
	if nowLast != nil {
		for _, b := range cbs.Blocks {
			if b.Equal(nowLast) {
				appendBlocks = true
			}
		}
	}

	if appendBlocks {
		bs.Blocks = append(bs.Blocks[:len(bs.Blocks) - 1], cbs.Blocks...)
	} else {
		bs.Blocks = append(bs.Blocks, cbs.Blocks...)
	}
	srcConfigPath := path.Join(hdfsBlocksDir, filename, hdfsBlocksFile)
	if err := saveToConfig(srcConfigPath, bs); err != nil {
		return err
	}
	if blocks != nil {
		t, err := opfsBlockGetUpdateTime(filename)
		if err != nil {
			panic(err)
		}
		blocks.Update = t
	}
	deferUnlock(filename, &errUnlock)

	return nil
}

var ErrInOpen error = errors.New("Not support delete file in open state")

func (obm *opfsBlocksMap) Delete(filename string) error {
	obm.Lock()
	defer obm.Unlock()
	_, ok := obm.constructmap[filename]
	if ok {
		return ErrInOpen
	}
	delete(obm.blocksmap, filename)
	deferUnlock(filename,&errUnlock)
	srcConfigPath := path.Join(hdfsBlocksDir, filename, hdfsBlocksFile)
	return opfs.RemovePath(srcConfigPath)
}

type opfsBlocksMapConf struct {
	defaultBlockSize uint64
	defaultReplicate int
	defaultReplicateMin int
}

func (obm *opfsBlocksMap) getDefaultBlockSize() uint64 {
	obm.RLock()
	defer obm.RUnlock()
	return obm.conf.defaultBlockSize
}

func (obm *opfsBlocksMap) getDefaultReplicate() int {
	obm.RLock()
	defer obm.RUnlock()
	return obm.conf.defaultReplicate
}

func (obm *opfsBlocksMap) getDefaultReplicateMin() int {
	obm.RLock()
	defer obm.RUnlock()
	return obm.conf.defaultReplicateMin
}

func NewOpfsBlocksMap(core hconf.HadoopConf) *opfsBlocksMap {
	globalLocks = &locksMap {
		locks: make(map[string]*opfsFileLock),
	}
	return &opfsBlocksMap {
		conf: &opfsBlocksMapConf {
			defaultBlockSize: core.ParseBlockSize(),
			defaultReplicate: core.ParseDfsReplicate(),
			defaultReplicateMin: core.ParseNamenodeReplicateMin(),
		},
		blocksmap: make(map[string]*opfsBlocks),
		constructmap: make(map[string]*opfsBlocks),
	}
}

var globalOpfsBlocksMap *opfsBlocksMap

func InitOpfsBlocksMap(core hconf.HadoopConf) error {
	globalOpfsBlocksMap = NewOpfsBlocksMap(core)
	return nil
}

func GetOpfsBlocksMap() *opfsBlocksMap {
	return globalOpfsBlocksMap
}
