package hadoopconf

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
)

type property struct {
	Name  string `xml:"name"`
	Value string `xml:"value"`
}

type propertyList struct {
	Property []property `xml:"property"`
}

var confFiles = []string{"core-site.xml", "hdfs-site.xml", "mapred-site.xml"}

type Conftype uint32

const (
	ConfNone  Conftype = iota
	ConfHadoopDir
	ConfOpfsConf
)

type HadoopConfEnv struct {
	Ctype Conftype
	ConfDir string
	Conf string
	*sync.RWMutex
}

func (h *HadoopConfEnv) getConfigPath(filename string) string {
	h.RLock()
	defer h.RUnlock()

	switch h.Ctype {
	case ConfNone:
		panic(fmt.Errorf("not init should be panic"))
	case ConfHadoopDir:
		return filepath.Join(h.ConfDir, filename)
	case ConfOpfsConf:
		return h.Conf
	default:
		panic(fmt.Errorf("not support type %v", h.Ctype))
	}

	return ""
}

func (h *HadoopConfEnv) load (filename string) (HadoopConf, error) {
	var conf HadoopConf = make(map[string]string)
	pList := propertyList{}
	configPath := h.getConfigPath(filename)
	f, err := ioutil.ReadFile(configPath)
	if os.IsNotExist(err) {
		return conf, nil
	} else if err != nil {
		return conf, err
	}

	err = xml.Unmarshal(f, &pList)
	if err != nil {
		return conf, err
	}

	for _, prop := range pList.Property {
		conf[prop.Name] = prop.Value
	}

	return conf, nil
}

const (
	HadoopAclServiceXml = "hadoop-policy.xml"
)

func (h *HadoopConfEnv) ReloadAclService() (HadoopConf, error) {
	return h.load(HadoopAclServiceXml)
}

func (h *HadoopConfEnv) ReloadCore() (HadoopConf, error) {
	res := HadoopConf{}
	for _, f := range confFiles {
		conf, err := h.load(f)
		if err != nil {
			return res, err
		}
		res = res.Merge(conf)
	}

	return res, nil
}

const (
	defaultOpfsHadoopConf = "/var/lib/openfs/cconf/cur/hdfs"

	osEnvHadoopConfDir = "HADOOP_CONF_DIR"
	osEnvHadoopHome = "HADOOP_HOME"
)

func loadFromEnvironment() string {
	confdir := ""
	hadoopConfDir := os.Getenv(osEnvHadoopConfDir)
	if hadoopConfDir != "" {
		confdir = hadoopConfDir
	}

	hadoopHome := os.Getenv(osEnvHadoopHome)
	if hadoopHome != "" {
		confdir = filepath.Join(hadoopHome, "conf")
	}

	return confdir
}

const (
	serviceAclxml = "hadoop-policy.xml"
)

func (he *HadoopConfEnv) ReloadServiceAcl() (HadoopConf, error) {
	return he.load(serviceAclxml)
}

func NewHadoopConfEnv() *HadoopConfEnv {
	res := &HadoopConfEnv {
		RWMutex: &sync.RWMutex{},
	}
	dir := loadFromEnvironment()
	if dir == "" {
		res.Ctype = ConfOpfsConf
		res.Conf = defaultOpfsHadoopConf
	} else {
		res.Ctype = ConfHadoopDir
		res.ConfDir = dir
	}

	return res
}

