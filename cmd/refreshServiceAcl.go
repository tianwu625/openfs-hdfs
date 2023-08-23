package cmd

import (
	"context"
	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
	"google.golang.org/protobuf/proto"
)

func refreshServiceAclDec(b []byte) (proto.Message, error) {
	req := new(hadoop.RefreshServiceAclRequestProto)
	return parseRequest(b, req)
}

func refreshServiceAcl(ctx context.Context,m proto.Message) (proto.Message, error) {
	req := m.(*hadoop.RefreshServiceAclRequestProto)
	return opfsRefreshServiceAcl(req)
}

func opfsRefreshServiceAcl(r *hadoop.RefreshServiceAclRequestProto) (*hadoop.RefreshServiceAclResponseProto, error) {
	core, err := globalConfEnv.ReloadCore()
	if err != nil {
		return nil, err
	}
	set := core.ParseEnableProtoAcl()
	if !set {
		globalClientProtoAcl.Set(set, nil)
		return &hadoop.RefreshServiceAclResponseProto{}, nil
	}
	conf, err := globalConfEnv.ReloadServiceAcl()
	if err != nil {
		return nil, err
	}
	scacl, err := conf.ParseClientProtocolAcl()
	if err != nil {
		return nil, err
	}
	globalClientProtoAcl.Set(set, scacl)

	return &hadoop.RefreshServiceAclResponseProto{}, nil
}
