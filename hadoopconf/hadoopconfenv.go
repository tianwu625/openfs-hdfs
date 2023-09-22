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

func (he *HadoopConfEnv) load(configPath string) (HadoopConf, error) {
	var conf HadoopConf = make(map[string]string)
	pList := propertyList{}
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


func (he *HadoopConfEnv) getConfType() Conftype {
	he.RLock()
	defer he.RUnlock()

	return he.Ctype
}

const (
	opfsDefaultXml = "/etc/default/opfs-default-hdfs.xml"
)

func (he *HadoopConfEnv) loadOpfsDefault() (HadoopConf, error) {
	return he.load(opfsDefaultXml)
}

func (he *HadoopConfEnv) getConfigDefaultPath(filename string) string {
	he.RLock()
	defer he.RUnlock()

	switch he.Ctype {
	case ConfNone:
		panic(fmt.Errorf("not init should be panic"))
	case ConfHadoopDir:
		return filepath.Join(filepath.Dir(filepath.Dir(he.ConfDir)), filename)
	case ConfOpfsConf:
		return filename
	default:
		panic(fmt.Errorf("not support type %v", he.Ctype))
	}

	return ""
}

var defaultConfFiles = []string{
	"share/doc/hadoop/hadoop-project-dist/hadoop-hdfs/hdfs-default.xml",
	"share/doc/hadoop/hadoop-project-dist/hadoop-common/core-default.xml",
	"share/doc/hadoop/hadoop-mapreduce-client/hadoop-mapreduce-client-core/mapred-default.xml",
}

var defaultOpfsConfFiles = []string {
	"/etc/default/hdfs-default/core-default.xml",
	"/etc/default/hdfs-default/hdfs-default.xml",
	"/etc/default/hdfs-default/mapred-default.xml",
}

func (he *HadoopConfEnv) loadHadoopDefault (filename string) (HadoopConf, error) {
	configPath := he.getConfigDefaultPath(filename)
	return he.load(configPath)
}

func (he *HadoopConfEnv) getDefaultCore() (HadoopConf, error) {
	res := HadoopConf{}
	t := he.getConfType()
	switch t {
	case ConfNone:
		panic(fmt.Errorf("type should be one of hadoop or opfs"))
	case ConfHadoopDir:
	for _, f := range defaultConfFiles {
		conf, err := he.loadHadoopDefault(f)
		if err != nil {
			return res, err
		}
		res = res.Merge(conf)
	}
	conf, err := he.loadOpfsDefault()
	if err != nil {
		return res, err
	}
	res = res.Merge(conf)
	case ConfOpfsConf:
	for _, f := range defaultOpfsConfFiles {
		conf, err := he.loadHadoopDefault(f)
		if err != nil  {
			return res, err
		}
		res = res.Merge(conf)
	}
	conf, err := he.loadOpfsDefault()
	if err != nil {
		return res, err
	}
	res = res.Merge(conf)
	default:
		panic(fmt.Errorf("not support this type %v", t))
	}

	return res, nil
}

var confFiles = []string{"core-site.xml", "hdfs-site.xml", "mapred-site.xml"}

func (he *HadoopConfEnv) loadHadoopConf (filename string) (HadoopConf, error) {
	configPath := he.getConfigPath(filename)
	return he.load(configPath)
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


func (he *HadoopConfEnv) getCore() (HadoopConf, error) {
	res := HadoopConf{}
	t := he.getConfType()
	switch t {
	case ConfNone:
		panic(fmt.Errorf("type should be one of hadoop or opfs"))
	case ConfHadoopDir:
	for _, f := range confFiles {
		conf, err := he.loadHadoopConf(f)
		if err != nil {
			return res, err
		}
		res = res.Merge(conf)
	}
	case ConfOpfsConf:
	for _, f := range confFiles {
		conf, err := he.loadHadoopConf(f)
		if err != nil {
			if os.IsNotExist(err) {
				return res, nil
			}
			return res, err
		}
		res = res.Merge(conf)
	}
	default:
		panic(fmt.Errorf("not support this type %v", t))
	}

	return res, nil
}


func (he *HadoopConfEnv) ReloadCore() (HadoopConf, error) {
	res := HadoopConf{}
	defaultCore, err := he.getDefaultCore()
	if err != nil {
		return res, err
	}
	res = res.Merge(defaultCore)
	core, err := he.getCore()
	if err != nil {
		return res, err
	}
	res = res.Merge(core)
	globalKeysVarMap = buildVarMap(res)
	return res, nil
}

const (
	HadoopPolicyXml = "hadoop-policy.xml"
)

func (he *HadoopConfEnv) ReloadServiceAcl() (HadoopConf, error) {
	return he.loadHadoopConf(HadoopPolicyXml)
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
		confdir = filepath.Join(hadoopHome, "etc", "hadoop")
	}

	return confdir
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

