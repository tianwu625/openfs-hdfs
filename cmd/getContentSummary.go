package cmd

import (
	"log"
	"path"
	"math"
	"errors"
	"io"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getContentSummaryDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetContentSummaryRequestProto)
	return parseRequest(b, req)
}

func getContentSummary(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetContentSummaryRequestProto)
	log.Printf("path %v", req.GetPath())
	return opfsGetContentSummary(req)
}

func opfsGetContentSummary(r *hdfs.GetContentSummaryRequestProto) (*hdfs.GetContentSummaryResponseProto, error) {
	res := new(hdfs.GetContentSummaryResponseProto)
	log.Printf("path %v", r.GetPath())
	src := r.GetPath()

	summary, err := opfsGetSummary(src)
	if err != nil {
		return res, err
	}

	res.Summary = summary

	return res, nil
}

const (
	defaultReplicate = 1
)

func opfsGetSummary(src string) (*hdfs.ContentSummaryProto, error) {
	dirs, files, allsize, err := opfsRecursivePath(src)
	if err != nil && !errors.Is(err, io.EOF){
		return new(hdfs.ContentSummaryProto), err
	}
	var (
		quotaNum uint64
		spaceQuota uint64
		consumeSpace uint64
	)
	quotaEntry, err := opfsGetQuota(src)
	if quotaEntry == nil {
		quotaNum, _, spaceQuota, consumeSpace = opfsGetdefaultQuota()
		//quto not set, consumespace can't get from file system
		consumeSpace += allsize * defaultReplicate
	} else {
		quotaNum, _, spaceQuota, consumeSpace = opfsGetquotaEntry(quotaEntry)
	}
	var infos *hdfs.StorageTypeQuotaInfosProto

	if src == "/" {
		infos = opfsGetRootQuotaInfo(allsize)
	}

	snapinfos, _ := opfsGetSnapInfo(src)
	ecpolicy, _ := opfsGetEcPolicy(src)

	res := &hdfs.ContentSummaryProto {
		Length: proto.Uint64(uint64(allsize)),
		FileCount:proto.Uint64(uint64(files)),
		DirectoryCount: proto.Uint64(uint64(dirs)),
		Quota: proto.Uint64(uint64(quotaNum)),
		SpaceConsumed: proto.Uint64(uint64(consumeSpace)),
		SpaceQuota:proto.Uint64(uint64(spaceQuota)),
		TypeQuotaInfos:infos,
		SnapshotLength: proto.Uint64(snapinfos.snapLength),
		SnapshotFileCount: proto.Uint64(snapinfos.snapfiles),
		SnapshotDirectoryCount: proto.Uint64(snapinfos.snapdirs),
		SnapshotSpaceConsumed: proto.Uint64(snapinfos.snapconsume),
		ErasureCodingPolicy: proto.String(ecpolicy),
	}

	return res, nil
}

func opfsRecursivePath(src string) (dircount uint64, filecount uint64, occupysize uint64, err error) {
	f, err := opfs.Open(src)
	if err != nil {
		return dircount, filecount, occupysize, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return dircount, filecount, occupysize, err
	}
	if fi.IsDir() {
		dircount++
		entries, err := f.Readdir(-1)
		if err != nil && !errors.Is(err, io.EOF){
			return dircount, filecount, occupysize, err
		}
		for _, e := range entries {
			if e.IsDir() {
				cdir, cfile, csize, err := opfsRecursivePath(path.Join(src, e.Name()))
				if err != nil {
					return dircount, filecount, occupysize, err
				}
				dircount += cdir
				filecount += cfile
				occupysize += csize
			} else {
				filecount++
				occupysize += uint64(e.Size())
			}
		}
	} else {
		filecount++
		occupysize += uint64(fi.Size())
	}
	return dircount, filecount, occupysize, nil
}

type hdfsQuotaEntry struct {
	quota uint64
	consume uint64
	spacequota uint64
	consumespace uint64
}

func opfsGetQuota(src string) (*hdfsQuotaEntry, error) {
	return nil, nil
}

func opfsGetdefaultQuota() (quota, consume, spaceQuota, consumeSpace uint64){
	return math.MaxUint64, 0, math.MaxUint64, 0
}

func opfsGetquotaEntry(entry *hdfsQuotaEntry)(quota, consume, spaceQuota, consumeSpace uint64) {
	return entry.quota, entry.consume, entry.spacequota, entry.consumespace
}

func opfsGetRootQuotaInfo(occupy uint64) (*hdfs.StorageTypeQuotaInfosProto){
	return &hdfs.StorageTypeQuotaInfosProto {
		TypeQuotaInfo: []*hdfs.StorageTypeQuotaInfoProto {
			&hdfs.StorageTypeQuotaInfoProto {
				Type: hdfs.StorageTypeProto_SSD.Enum(),
				Quota: proto.Uint64(math.MaxUint64),
				Consumed: proto.Uint64(0),
			},
			&hdfs.StorageTypeQuotaInfoProto {
				Type: hdfs.StorageTypeProto_ARCHIVE.Enum(),
				Quota: proto.Uint64(math.MaxUint64),
				Consumed: proto.Uint64(0),
			},
			&hdfs.StorageTypeQuotaInfoProto {
				Type: hdfs.StorageTypeProto_RAM_DISK.Enum(),
				Quota: proto.Uint64(math.MaxUint64),
				Consumed: proto.Uint64(0),
			},
			&hdfs.StorageTypeQuotaInfoProto {
				Type: hdfs.StorageTypeProto_DISK.Enum(),
				Quota: proto.Uint64(math.MaxUint64),
				Consumed: proto.Uint64(0),
			},
			&hdfs.StorageTypeQuotaInfoProto {
				Type: hdfs.StorageTypeProto_PROVIDED.Enum(),
				Quota: proto.Uint64(math.MaxUint64),
				Consumed: proto.Uint64(occupy),
			},
		},
	}
}

type snapInfo struct {
	snapLength uint64
	snapfiles uint64
	snapdirs uint64
	snapconsume uint64
}

func opfsGetSnapInfo(src string) (*snapInfo, error) {
	return &snapInfo {
		snapLength: 0,
		snapfiles: 0,
		snapdirs: 0,
		snapconsume: 0,
	}, nil
}

func opfsGetEcPolicy(src string) (string, error) {
	return "Replicated", nil
}
