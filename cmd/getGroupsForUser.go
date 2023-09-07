package cmd

import (
	"context"
	"log"

	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
)

func getGroupsForUserDec(b []byte) (proto.Message, error) {
	req := new(hadoop.GetGroupsForUserRequestProto)
	return parseRequest(b, req)
}

func getGroupsForUser(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hadoop.GetGroupsForUserRequestProto)
	log.Printf("user %v", req.GetUser())
	return opfsGetGroupsForUser(req)
}

func opfsGetGroupsForUser(r *hadoop.GetGroupsForUserRequestProto) (*hadoop.GetGroupsForUserResponseProto, error) {
	user := r.GetUser()
	giams := getGlobalIAM()
	groups, err := giams.GetGroupsByUser(user)
	if err != nil {
		return nil, err
	}

	return &hadoop.GetGroupsForUserResponseProto {
		Groups: groups,
	}, nil
}
