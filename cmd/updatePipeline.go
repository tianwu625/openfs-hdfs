package cmd

import (
	"log"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func updatePipelineDec(b []byte) (proto.Message, error) {
	req := new(hdfs.UpdatePipelineRequestProto)
	return parseRequest(b, req)
}

func updatePipeline(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.UpdatePipelineRequestProto)
	log.Printf("clientName %v\nold Block %v\nNew Block %v\nNewNodes %v\nstorageIds %v\n",
		     req.GetClientName(), req.GetOldBlock(), req.GetNewBlock(), req.GetNewNodes(), req.GetStorageIDs())
	return opfsUpdatePipeline(req)
}

func opfsUpdatePipeline(r *hdfs.UpdatePipelineRequestProto) (*hdfs.UpdatePipelineResponseProto, error) {
	res := new(hdfs.UpdatePipelineResponseProto)

	return res, nil
}

