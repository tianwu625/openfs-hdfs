package cmd

import (
	"log"
	"fmt"
	"time"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func rollingUpgradeDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RollingUpgradeRequestProto)
	return parseRequest(b, req)
}

func rollingUpgrade(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RollingUpgradeRequestProto)
	res, err := opfsRollingUpgrade(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}


var (
	Stime time.Time = time.Unix(0,0)
	Etime time.Time = time.Unix(0,0)
	finalized = false
)

func opfsRollingQuery() (*hdfs.RollingUpgradeInfoProto, error){
	return &hdfs.RollingUpgradeInfoProto {
		Status: &hdfs.RollingUpgradeStatusProto {
			BlockPoolId: proto.String("/"),
			Finalized:proto.Bool(finalized),
		},
		StartTime: proto.Uint64(uint64(Stime.UnixMilli())),
		FinalizeTime: proto.Uint64(uint64(Etime.UnixMilli())),
		CreatedRollbackImages: proto.Bool(false),
	}, nil
}

func opfsRollingStart() (*hdfs.RollingUpgradeInfoProto, error) {
	Stime = time.Now()
	return &hdfs.RollingUpgradeInfoProto {
		Status: &hdfs.RollingUpgradeStatusProto {
			BlockPoolId: proto.String("/"),
			Finalized:proto.Bool(finalized),
		},
		StartTime:proto.Uint64(uint64(Stime.UnixMilli())),
		FinalizeTime: proto.Uint64(uint64(Etime.UnixMilli())),
		CreatedRollbackImages: proto.Bool(false),
	}, nil
}

func opfsRollingFinalize() (*hdfs.RollingUpgradeInfoProto, error) {
	Etime = time.Now()
	finalized = true

	return &hdfs.RollingUpgradeInfoProto {
		Status: &hdfs.RollingUpgradeStatusProto {
			BlockPoolId: proto.String("/"),
			Finalized:proto.Bool(finalized),
		},
		StartTime:proto.Uint64(uint64(Stime.UnixMilli())),
		FinalizeTime: proto.Uint64(uint64(Etime.UnixMilli())),
		CreatedRollbackImages: proto.Bool(false),
	}, nil
}

func opfsRollingUpgrade(r *hdfs.RollingUpgradeRequestProto) (*hdfs.RollingUpgradeResponseProto, error) {
	action := r.GetAction().String()
	log.Printf("get action %v", action)
	var (
		info *hdfs.RollingUpgradeInfoProto
		err error
	)
	switch action {
	case hdfs.RollingUpgradeActionProto_QUERY.String():
		info, err = opfsRollingQuery()
		if err != nil {
			return nil, err
		}
	case hdfs.RollingUpgradeActionProto_START.String():
		info, err = opfsRollingStart()
		if err != nil {
			return nil, err
		}
	case hdfs.RollingUpgradeActionProto_FINALIZE.String():
		info, err = opfsRollingFinalize()
		if err != nil {
			return nil, err
		}
	default:
		panic(fmt.Errorf("not support action %v", action))
	}

	return &hdfs.RollingUpgradeResponseProto {
		RollingUpgradeInfo: info,
	}, nil
}
