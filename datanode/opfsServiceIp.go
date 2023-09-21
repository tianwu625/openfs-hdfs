package datanode

import (
	"os"
	"io"

	jsoniter "github.com/json-iterator/go"
	hconf "github.com/openfs/openfs-hdfs/hadoopconf"
)

type opfsServiceIp struct {
	Ip string `json:"serviceIp, omitempty"`
	Hostname string `json:"hostname, omitempty"`
}


const (
	serviceIpConfig = "/etc/default/opfsServiceIp"
)

func getOpfsServiceIpFromConfig() (addr string, hostname string) {
	f, err := os.Open(serviceIpConfig)
	if err != nil {
		return "", ""
	}

	b, err := io.ReadAll(f)
	if err != nil {
		return "", ""
	}
	json := jsoniter.ConfigCompatibleWithStandardLibrary
	res := new(opfsServiceIp)
	if err = json.Unmarshal(b, res); err != nil {
		return "", ""
        }

	return res.Ip, res.Hostname
}


func setOpfsServiceIpToDatanodeId(res *hconf.DatanodeId) {
	addr, name := getOpfsServiceIpFromConfig()
	if addr != "" {
		res.IpAddr = addr
	}

	if name != "" {
		res.Hostname = name
	}
}
