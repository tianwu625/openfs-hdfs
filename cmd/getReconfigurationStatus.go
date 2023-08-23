package cmd

import (
	"context"
	"time"
	"log"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func getReconfigurationStatusDec(b []byte) (proto.Message, error) {
	req := new(hdfs.GetReconfigurationStatusRequestProto)
	return parseRequest(b, req)
}

func getReconfigurationStatus(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.GetReconfigurationStatusRequestProto)
	return opfsGetReconfigurationStatus(req)
}

func opfsGetReconfigurationStatus(r *hdfs.GetReconfigurationStatusRequestProto)(*hdfs.GetReconfigurationStatusResponseProto, error) {
	e := globalReconfig.GetStatus()
	log.Printf("event e %v", e)
	start := int64(0)
	if !e.Start.Equal(time.Unix(0,0)) {
		start = e.Start.UnixMilli()
	}
	end := int64(0)
	if !e.End.Equal(time.Unix(0,0)) {
		end = e.End.UnixMilli()
	}

	changes := make([]*hdfs.GetReconfigurationStatusConfigChangeProto, 0, len(e.Oldconf))
	for k, v := range e.Oldconf {
		nv := e.Newconf[k]
		err := e.Errs[k]
		c := &hdfs.GetReconfigurationStatusConfigChangeProto {
			Name: proto.String(k),
			OldValue: proto.String(v),
			NewValue: proto.String(nv),
		}
		if err != nil {
			c.ErrorMessage = proto.String(err.Error())
		}

		changes = append(changes, c)
	}

	return &hdfs.GetReconfigurationStatusResponseProto {
		StartTime: proto.Int64(start),
		EndTime: proto.Int64(end),
		Changes: changes,
	}, nil
}
