package cmd

import (
	"errors"
	"log"
	"math"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func setQuotaDec(b []byte) (proto.Message, error) {
	req := new(hdfs.SetQuotaRequestProto)
	return parseRequest(b, req)
}

func setQuota(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.SetQuotaRequestProto)
	res, err := opfsSetQuota(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func opfsCheckStorageType(stype string) bool {
	switch stype {
	case hdfs.StorageTypeProto_DISK.String():
		return true
	case hdfs.StorageTypeProto_SSD.String():
		fallthrough
	case hdfs.StorageTypeProto_ARCHIVE.String():
		fallthrough
	case hdfs.StorageTypeProto_RAM_DISK.String():
		fallthrough
	case hdfs.StorageTypeProto_PROVIDED.String():
		fallthrough
	case hdfs.StorageTypeProto_NVDIMM.String():
		fallthrough
	default:
		return false
	}

	return false
}

func opfsSetNamespaceQuota(src string, setQuota bool, nsQuota uint64) error {
	quota := opfsHdfsNamespaceQuota {
		SetQuota: setQuota,
		Quota: nsQuota,
	}

	return globalMeta.SetNamespaceQuota(src, quota)
}

var errNotSupportStorageType error = errors.New("openfs only support disk type for now")

const (
	noSetValue = math.MaxInt64
	//max uint64 means no limit, also means clean all quota
	setClrValue = math.MaxUint64
)

func opfsSetSpaceQuota(src string, quota uint64) error {
	q := &opfs.OpfsQuotaEntry {
		Spacequota: quota,
	}
	return opfs.SetSpaceQuota(src, q)
}


func opfsQuotaSet(src string, nsQuota, ssQuota uint64, stype string) error {

	if ssQuota != noSetValue {
		if !opfsCheckStorageType(stype) {
			return errNotSupportStorageType
		}
		if ssQuota == setClrValue {
			return opfsSetSpaceQuota(src, 0)
		}
		return opfsSetSpaceQuota(src, ssQuota)
	}

	if nsQuota != noSetValue {
		log.Printf("nsQuota %v noSetValue %v, nsQuota == noSetValue %v",
			    nsQuota, noSetValue, nsQuota == noSetValue)
		if nsQuota == setClrValue {
			return opfsSetNamespaceQuota(src, false, nsQuota)
		}
		return opfsSetNamespaceQuota(src, true, nsQuota)
	}

	return nil
}


func opfsSetQuota(r *hdfs.SetQuotaRequestProto) (*hdfs.SetQuotaResponseProto, error) {
	src := r.GetPath()
	nsQuota := r.GetNamespaceQuota()
	ssQuota := r.GetStoragespaceQuota()
	ssType := r.GetStorageType().String()

	log.Printf("SetQuota:::src %v, nsQuota %v, ssQuota %v, ssType %v", src,
                     nsQuota, ssQuota, ssType)

	if err := opfsQuotaSet(src, nsQuota, ssQuota, ssType); err != nil {
		return nil, err
	}

	return new(hdfs.SetQuotaResponseProto), nil
}
