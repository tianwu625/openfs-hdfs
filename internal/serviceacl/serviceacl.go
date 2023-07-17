package serviceacl

import (
	"net"
	"log"
)

type ServiceAcl struct {
	AllowUsers []string
	AllowGroups []string
	DenyUsers []string
	DenyGroups []string
	AllowIps []net.IP
	AllowHosts string
	AllowIpNets []net.IPNet
	DenyIps []net.IP
	DenyHosts string
	DenyIpNets []net.IPNet
}

func (s *ServiceAcl) Clone() *ServiceAcl {
	c := ServiceAcl {
		AllowUsers: make([]string, len(s.AllowUsers)),
		AllowGroups: make([]string, len(s.AllowGroups)),
		DenyUsers: make([]string, len(s.DenyUsers)),
		DenyGroups: make([]string, len(s.DenyGroups)),
		AllowIps: make([]net.IP, 0, len(s.AllowIps)),
		AllowHosts: s.AllowHosts,
		AllowIpNets: make([]net.IPNet, 0, len(s.AllowIpNets)),
		DenyIps: make([]net.IP, 0, len(s.DenyIps)),
		DenyHosts: s.DenyHosts,
		DenyIpNets: make([]net.IPNet, 0, len(s.DenyIpNets)),
	}
	copy(c.AllowUsers, s.AllowUsers)
	copy(c.AllowGroups, s.AllowGroups)
	copy(c.DenyUsers, s.DenyUsers)
	copy(c.DenyGroups, s.DenyGroups)
	for _, ip := range s.AllowIps {
		c.AllowIps = append(c.AllowIps, net.ParseIP(ip.String()))
	}
	for _, ipnet := range s.AllowIpNets {
		_, net, _ := net.ParseCIDR(ipnet.String())
		c.AllowIpNets = append(c.AllowIpNets, *net)
	}
	for _, ip := range s.DenyIps {
		c.DenyIps = append(c.DenyIps, net.ParseIP(ip.String()))
	}
	for _, ipnet := range s.DenyIpNets {
		_, net, _ := net.ParseCIDR(ipnet.String())
		c.DenyIpNets = append(c.DenyIpNets, *net)
	}

	return &c
}

const (
	authAll = "*"
	authNone = ""
)

func getGroups(user string) []string {
	groups := []string{}

	return groups
}

func (s *ServiceAcl) checkuser(user string) bool {
	if s.AllowUsers[0] == authAll {
		return true
	}

	for _, u := range s.AllowUsers {
		if u == user {
			return true
		}
	}

	if s.AllowGroups[0] == authAll {
		return true
	}

	groups := getGroups(user)

	for _, g := range s.AllowGroups {
		for _, ug := range groups {
			if g == ug {
				return true
			}
		}
	}

	if s.DenyUsers[0] == authNone {
		return true
	}

	for _, u := range s.DenyUsers {
		if u == user {
			return false
		}
	}

	if s.DenyGroups[0] == authNone {
		return true
	}

	for _, g := range s.DenyGroups {
		for _, ug := range groups {
			if g == ug {
				return false
			}
		}
	}

	return false
}


func (s *ServiceAcl) checkip(ip string) bool {
	if s.AllowHosts == authAll {
		return true
	}

	tip := net.ParseIP(ip)
	for _, ip := range s.AllowIps {
		if ip.Equal(tip) {
			return true
		}
	}
	for _, net := range s.AllowIpNets {
		if net.Contains(tip) {
			return true
		}
	}

	if s.DenyHosts == authNone {
		return true
	}

	for _, ip := range s.DenyIps {
		if ip.Equal(tip) {
			return false
		}
	}

	for _, net := range s.DenyIpNets {
		if net.Contains(tip) {
			return false
		}
	}

	//default is deny
	return false
}

func (s *ServiceAcl) CheckAllow (user, ip string) bool {
	log.Printf("s %v", s)
	return s.checkuser(user) && s.checkip(ip)
}
