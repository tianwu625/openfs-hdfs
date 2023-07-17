package hadoopconf

import (
	"strings"
	"strconv"
	"errors"
	"net"
	"log"

	sacl "github.com/openfs/openfs-hdfs/internal/serviceacl"
)

type HadoopConf map[string]string


var errInvalid = errors.New("Invalid value of property")

const (
	SecurityClientProtocolAcl = "security.client.protocol.acl"
	SecurityServiceAuthDefaultAcl = "security.service.authorization.default.acl"
	SecurityClientProtocolAclBlocked = SecurityClientProtocolAcl + ".blocked"
	SecurityServiceAuthDefaultAclBlocked = SecurityServiceAuthDefaultAcl + ".blocked"
	SecurityClientProtocolHost = "security.client.protocol.hosts"
	SecurityServiceAuthDefaultHosts = "security.service.authorization.default.hosts"
	SecurityClientProtocolHostBlocked = SecurityClientProtocolHost + ".blocked"
	SecurityServiceAuthDefaultHostsBlocked = SecurityServiceAuthDefaultHosts + ".blocked"
)

func parseAuth(authstr string) ([]string, []string, error) {
	res := strings.Split(authstr, " ")
	if len(res) != 2 {
		return []string{}, []string{}, errInvalid
	}
	userstr := res[0]
	users := strings.Split(userstr, ",")
	groupstr := res[1]
	groups := strings.Split(groupstr, ",")

	return users, groups, nil
}

func parseAuthIp(authstr string) ([]net.IP, []net.IPNet, error) {
	ss := strings.Split(authstr, ",")
	ips := make([]net.IP, 0, len(ss))
	ipnets := make([]net.IPNet, 0, len(ss))
	for _, s := range ss {
		if ip := net.ParseIP(s); ip != nil {
			ips = append(ips, ip)
		} else if addrs, err := net.LookupHost(s);err == nil {
			for _, addr := range addrs {
				ip := net.ParseIP(addr)
				ips = append(ips, ip)
			}
		} else if _, ipNet, err := net.ParseCIDR(s); err == nil {
			ipnets = append(ipnets, *ipNet)
		} else {
			return ips, ipnets, errInvalid
		}
	}

	return ips, ipnets, nil
}

const (
	defaultAllowAuth = "*"
	defaultDenyAuth = ""
)

func (h HadoopConf) getValueWithDefault(key,defaultKey,defaultValue string) string {
	s, ok := h[key]
	if !ok {
		s, ok = h[defaultKey]
		if !ok {
			s = defaultValue
		}
	}

	return s
}

func (h HadoopConf) ParseClientProtocolAcl() (*sacl.ServiceAcl, error) {
	authstr := h.getValueWithDefault(SecurityClientProtocolAcl, SecurityServiceAuthDefaultAcl, defaultAllowAuth)
	users := []string{defaultAllowAuth}
	groups := []string{defaultAllowAuth}
	var err error
	if authstr != defaultAllowAuth {
		users, groups, err = parseAuth(authstr)
		if err != nil {
			return nil, err
		}
	}
	authstr = h.getValueWithDefault(SecurityClientProtocolAclBlocked, SecurityClientProtocolAclBlocked, defaultDenyAuth)
	busers := []string{""}
	bgroups := []string{""}
	if authstr != defaultDenyAuth {
		busers, bgroups, err = parseAuth(authstr)
		if err != nil {
			return nil, err
		}
	}
	hosts := defaultAllowAuth
	ips := []net.IP{}
	ipnets := []net.IPNet{}
	authstr = h.getValueWithDefault(SecurityClientProtocolHost, SecurityServiceAuthDefaultHosts, defaultAllowAuth)
	if authstr != defaultAllowAuth {
		ips, ipnets, err = parseAuthIp(authstr)
		if err != nil {
			return nil, err
		}
		hosts = defaultDenyAuth
	}

	bhosts := defaultDenyAuth
	bips := []net.IP{}
	bipnets := []net.IPNet{}
	authstr = h.getValueWithDefault(SecurityClientProtocolHostBlocked, SecurityServiceAuthDefaultHostsBlocked, defaultDenyAuth)
	if authstr != defaultDenyAuth {
		bips, bipnets, err = parseAuthIp(authstr)
		if err != nil {
			return nil, err
		}
		bhosts = defaultAllowAuth
	}

	return &sacl.ServiceAcl {
		AllowUsers: users,
		AllowGroups: groups,
		DenyUsers: busers,
		DenyGroups: bgroups,
		AllowIps: ips,
		AllowHosts: hosts,
		AllowIpNets: ipnets,
		DenyIps:bips,
		DenyHosts: bhosts,
		DenyIpNets: bipnets,
	}, nil
}

func (h HadoopConf) Merge(n HadoopConf) HadoopConf {
	for k, v := range n {
		h[k] = v
	}

	return h
}

const (
	HadoopSecurityAuth = "hadoop.security.authorization"
)

func (h HadoopConf) ParseEnableProtoAcl() bool {
	v, ok := h[HadoopSecurityAuth]
	if !ok {
		return false
	}

	b, err := strconv.ParseBool(strings.ToLower(v))
	if err != nil {
		log.Printf("parse failed value %v", v)
		return false
	}

	return b
}
