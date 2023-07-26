package cmd

import (
	"log"
	"errors"

	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
)

func refreshDec(b []byte) (proto.Message, error) {
	req := new(hadoop.GenericRefreshRequestProto)
	return parseRequest(b, req)
}

func refresh(m proto.Message) (proto.Message, error) {
	req := m.(*hadoop.GenericRefreshRequestProto)
	res, err := opfsRefresh(req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

type keyFunc func(args []string) (message, sendername string, err error)

var globalUserKeyMap map[string]keyFunc

func init() {
	globalUserKeyMap = make(map[string]keyFunc)
}

func errToExistCode(err error) int32 {
	if err != nil {
		return 1
	}

	return 0
}

var ErrNoRefresh error = errors.New("No Refresh func")

func opfsRefresh(r *hadoop.GenericRefreshRequestProto) (*hadoop.GenericRefreshResponseProto, error) {
	id := r.GetIdentifier()
	args := r.GetArgs()

	log.Printf("id %v, args %v", id, args)

	f := globalUserKeyMap[id]
	if f == nil {
		log.Printf("no found the refresh func %v", id)
		return nil, ErrNoRefresh
	}
	message, sender, err := f(args)
	if err != nil {
		return nil, err
	}

	return &hadoop.GenericRefreshResponseProto {
		ExitStatus: proto.Int32(0),
		UserMessage: proto.String(message),
		SenderName: proto.String(sender),
	}, nil
}
