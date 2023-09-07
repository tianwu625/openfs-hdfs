package cmd

import (
	"context"
	"log"
	"fmt"

	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
	"github.com/openfs/openfs-hdfs/internal/fsmeta"
)

func setSafeModeDec(b []byte) (proto.Message, error) {
	req := new(hdfs.SetSafeModeRequestProto)
	return parseRequest(b, req)
}

func setSafeMode(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.SetSafeModeRequestProto)
	log.Printf("Action %v\nchecked %v\n", req.GetAction(), req.GetChecked())
	return opfsSetSafeMode(req)
}


func opfsSetSafeMode(r *hdfs.SetSafeModeRequestProto) (*hdfs.SetSafeModeResponseProto, error) {
	action := r.GetAction()
	check := r.GetChecked()

	log.Printf("action %v, check %v", action, check)

	modeNormal := fsmeta.ModeNormal
	modeSafe := fsmeta.ModeSafe
	mode := modeNormal
	gfs := fsmeta.GetGlobalFsMeta()
	switch action.String() {
	case hdfs.SafeModeActionProto_SAFEMODE_LEAVE.String():
		gfs.SetMode(modeNormal)
		mode = gfs.GetMode()
	case hdfs.SafeModeActionProto_SAFEMODE_ENTER.String():
		gfs.SetMode(modeSafe)
		mode = gfs.GetMode()
	case hdfs.SafeModeActionProto_SAFEMODE_GET.String():
		mode = gfs.GetMode()
	case hdfs.SafeModeActionProto_SAFEMODE_FORCE_EXIT.String():
		gfs.SetMode(modeNormal)
		mode = gfs.GetMode()

	default:
		panic(fmt.Errorf("not support action %v", action.String()))
	}

	res := &hdfs.SetSafeModeResponseProto {
		Result: proto.Bool(mode == modeSafe),
	}

	return res, nil
}


