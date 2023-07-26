package cmd

import (
	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
)

func refreshUserToGroupsMappingsDec(b []byte) (proto.Message, error) {
	req := new(hadoop.RefreshUserToGroupsMappingsRequestProto)
	return parseRequest(b, req)
}

func refreshUserToGroupsMappings(m proto.Message) (proto.Message, error) {
	req := m.(*hadoop.RefreshUserToGroupsMappingsRequestProto)
	return opfsRefreshUserToGroupsMappings(req)
}

func opfsRefreshUserToGroupsMappings(r *hadoop.RefreshUserToGroupsMappingsRequestProto) (*hadoop.RefreshUserToGroupsMappingsResponseProto, error) {
	err := globalIAMSys.LoadUsers()
	if err != nil {
		return nil, err
	}

	return new(hadoop.RefreshUserToGroupsMappingsResponseProto), nil
}
