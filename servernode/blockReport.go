package servernode

import (
	"log"
	"context"

	hdsp "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_server"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/rpc"
	"github.com/openfs/openfs-hdfs/internal/fsmeta"
)

func blockReportDec(b []byte) (proto.Message, error) {
	req := new(hdsp.BlockReportRequestProto)

	return rpc.ParseRequest(b, req)
}

func blockReport(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdsp.BlockReportRequestProto)

	log.Printf("blockReport req %v", req)

	res, err := opfsBlockReport(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}
/*
cmd:{cmdType:FinalizeCommand  finalizeCmd:{blockPoolId:"BP-2038318745-192.168.21.164-1690428558063"}}
*/
func opfsBlockReport(r *hdsp.BlockReportRequestProto) (*hdsp.BlockReportResponseProto, error) {
	poolId := r.GetBlockPoolId()
	var datanodeCmd *hdsp.DatanodeCommandProto
	if r.GetContext().GetTotalRpcs() == r.GetContext().GetCurRpc() + 1 {
		cmd := &hdsp.FinalizeCommandProto {
			BlockPoolId: proto.String(poolId),
		}
		datanodeCmd = &hdsp.DatanodeCommandProto {
			CmdType:hdsp.DatanodeCommandProto_FinalizeCommand.Enum(),
			FinalizeCmd: cmd,
		}
		//maybe need to different to process case
		gfs := fsmeta.GetGlobalFsMeta()
		gfs.SetMode(fsmeta.ModeNormal)
		log.Printf("leave safe mode!!!")
	}
	return &hdsp.BlockReportResponseProto{
		Cmd: datanodeCmd,
	}, nil
}
