package cmd

import (
	"sync"
	"log"
	"syscall"

	sacl "github.com/openfs/openfs-hdfs/internal/serviceacl"
	"github.com/openfs/openfs-hdfs/internal/rpc"
)

type serviceAclConf struct {
	Enable bool
	*sync.RWMutex
	*sacl.ServiceAcl
}

func (sac *serviceAclConf) Set (enable bool, acl *sacl.ServiceAcl) error {
	sac.Lock()
	defer sac.Unlock()

	sac.Enable = enable
	if acl != nil {
		sac.ServiceAcl = acl.Clone()
	} else {
		sac.ServiceAcl = nil
	}

	return nil
}

func (sac *serviceAclConf) CheckAllow(user, ip string) bool {
	sac.RLock()
	defer sac.RUnlock()

	if !sac.Enable {
		log.Printf("disable sac %v", sac)
		return true
	}
	log.Printf("enable sac %v", sac)
	return sac.ServiceAcl.CheckAllow(user, ip)
}

func (sac *serviceAclConf) HandshakeAfter(client *rpc.RpcClient) error {
	if !sac.CheckAllow(client.User, client.ClientIp) {
		return syscall.EPERM
	}

	return nil
}


func NewServiceAclConf() *serviceAclConf {
	return &serviceAclConf {
		Enable: false,
		RWMutex: &sync.RWMutex{},
		ServiceAcl: &sacl.ServiceAcl{},
	}
}
