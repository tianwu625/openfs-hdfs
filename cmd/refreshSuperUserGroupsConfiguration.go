package cmd

import (
	"context"
	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
)

func refreshSuperUserGroupsConfigurationDec(b []byte) (proto.Message, error) {
	req := new(hadoop.RefreshSuperUserGroupsConfigurationRequestProto)
	return parseRequest(b, req)
}

func refreshSuperUserGroupsConfiguration(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hadoop.RefreshSuperUserGroupsConfigurationRequestProto)
	return opfsRefreshSuperUserGroupsConfiguration(req)
}

func opfsRefreshSuperUserGroupsConfiguration(r *hadoop.RefreshSuperUserGroupsConfigurationRequestProto) (*hadoop.RefreshSuperUserGroupsConfigurationResponseProto, error) {
	core, err := globalConfEnv.ReloadCore()
	if err != nil {
		return nil, err
	}
	groups, err := core.ParseRootGroups()
	if err != nil {
		return nil, err
	}

	if err := globalIAMSys.ReloadRootGroups(groups); err != nil {
		return nil, err
	}

	return new(hadoop.RefreshSuperUserGroupsConfigurationResponseProto), nil
}
