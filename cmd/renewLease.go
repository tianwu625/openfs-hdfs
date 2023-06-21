package cmd

import (
	"fmt"
	"time"
	"log"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func renewLeaseDec(b []byte) (proto.Message, error) {
	req := new(hdfs.RenewLeaseRequestProto)
	return parseRequest(b, req)
}

func renewLease(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.RenewLeaseRequestProto)
	log.Printf("client %v", req.GetClientName())
	return opfsRenewLease(req)
}

type clientInfo struct {
	clientName string
	update time.Time
}

type clientInfos struct {
	infos map[string]*clientInfo
}

var globalClientInfos *clientInfos = nil

func init() {
	globalClientInfos = &clientInfos {
		infos: make(map[string]*clientInfo),
	}
}

func opfsRenewLease(r *hdfs.RenewLeaseRequestProto) (*hdfs.RenewLeaseResponseProto, error) {
	res := new(hdfs.RenewLeaseResponseProto)
	if globalClientInfos == nil {
		panic(fmt.Sprintf("should be impossible, init should be call"))
	}

	name := r.GetClientName()

	info, ok := globalClientInfos.infos[name]
	if !ok {
		info = &clientInfo {
			clientName:name,
		}
		globalClientInfos.infos[name] = info
	}
	info.update = time.Now()
	/*
	log.Printf("info name %v, time %v", globalClientInfos.infos[name].clientName,
			globalClientInfos.infos[name].update.String())
	*/
	return res, nil
}
