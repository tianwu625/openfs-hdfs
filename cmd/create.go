package cmd

import (
	"log"
	"os"
	pathutils "path"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)


func createDec(b []byte) (proto.Message, error) {
	req := new(hdfs.CreateRequestProto)
	return parseRequest(b, req)
}

func create(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.CreateRequestProto)
	log.Printf("src %v\nmask %o\nclient %v\ncreateFlag %v\ncreateParent %v\n", req.GetSrc(), req.GetMasked().GetPerm(),
		   req.GetClientName(), req.GetCreateFlag(), req.GetCreateParent())
	log.Printf("replication %v\nblocksize %v\ncrypto %v\numask %o\necpolicy %v\nstoragep %v\n",
		   req.GetReplication(), req.GetBlockSize(), req.GetCryptoProtocolVersion(),
	           req.GetUnmasked().GetPerm(), req.GetEcPolicyName(), req.GetStoragePolicy())
	return opfsCreate(req)
}

func opfsCreate(r *hdfs.CreateRequestProto) (*hdfs.CreateResponseProto, error) {
	res := new(hdfs.CreateResponseProto)
	createParent := r.GetCreateParent()
	src := r.GetSrc()
	perm := r.GetMasked().GetPerm() & allperm
	if perm == 0 {
		perm = ^(r.GetUnmasked().GetPerm()) & allperm
	}
	flag := r.GetCreateFlag()

	log.Printf("perm %v\nflag %v\n", perm, flag)

	if createParent {
		if err := opfs.MakeDirAll(pathutils.Dir(src), os.FileMode(perm)); err != nil {
			log.Printf("mkdir fail %v\n", err)
			return res, err
		}
	}
	f, err := opfs.OpenWithCreate(src, os.O_CREATE, os.FileMode(perm))
	if err != nil {
		return res, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return res, err
	}
	st := opfsHdfsFileStatus(src, fi, nil)
	res.Fs = st

	return res, nil
}
