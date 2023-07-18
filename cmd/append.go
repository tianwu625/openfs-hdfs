package cmd

import (
	"log"
	"os"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
)

func appendFileDec(b []byte) (proto.Message, error) {
	req := new(hdfs.AppendRequestProto)
	return parseRequest(b, req)
}

func appendFile(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.AppendRequestProto)
	log.Printf("src %v\nclientName %v\nFlag %v\n", req.GetSrc(), req.GetClientName(), req.GetFlag())
	checkPosixPermission(&posixCheckPermissionRequest {
		absPath: req.GetSrc(),
		action: actionAppend,
		user: "",
		groups: []string{},
	})
	return opfsAppendFile(req)
}

func getBlockToken() (*hadoop.TokenProto) {
	token := &hadoop.TokenProto {
		Identifier:[]byte{},
		Password:[]byte{},
		Kind:proto.String(""),
		Service: proto.String(""),
	}

	return token
}

func getLastBlock(src string) (*hdfs.LocatedBlockProto, error) {
	size, err := getFileSize(src)
	if err != nil {
		return nil, err
	}
	//last Block is Full or empty file
	if size % defaultBlockSize == 0 {
		return nil, nil
	}

	eb := &hdfs.ExtendedBlockProto {
		PoolId: proto.String(src),
		BlockId: proto.Uint64(uint64(size/defaultBlockSize)),
		GenerationStamp: proto.Uint64(1000),
		NumBytes: proto.Uint64(uint64(size%defaultBlockSize)),
	}

	lb := &hdfs.LocatedBlockProto {
		B: eb,
		Offset: proto.Uint64(eb.GetBlockId() * defaultBlockSize),
		Locs:getDatanodeInfo(eb),
		Corrupt: proto.Bool(false),
		BlockToken: getBlockToken(),
		IsCached: []bool {
			false,
		},
		StorageTypes: []hdfs.StorageTypeProto {
			hdfs.StorageTypeProto_DISK,
		},
		StorageIDs: []string {
			"openfs-xx",
		},
	}
	return lb, nil
}

func opfsAppendFile(r *hdfs.AppendRequestProto) (*hdfs.AppendResponseProto, error) {
	res := new(hdfs.AppendResponseProto)
	src := r.GetSrc()
	clientname := r.GetClientName()
	flag := r.GetFlag()

	log.Printf("src %v, client %v, flag %v", src, clientname, flag)
	var err error
	res.Stat, err = opfsHdfsStat(src)
	if err != nil {
		if os.IsNotExist(err) && flag != 0{
			log.Printf("do create ?")
		} else {
			return res, err
		}
	}

	res.Block, err = getLastBlock(src)
	if err != nil {
		return res, err
	}


	return res, nil
}
