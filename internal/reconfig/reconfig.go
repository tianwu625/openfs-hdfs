package reconfig

import (
	"sync"
	"time"

	"github.com/openfs/openfs-hdfs/hadoopconf"
)

type ReconfigOnline struct {
	*sync.RWMutex
	start time.Time
	end time.Time
	oldconf hadoopconf.HadoopConf
	conf hadoopconf.HadoopConf
}

type ReconfigEvent struct {
	Start time.Time
	End time.Time
	Oldconf hadoopconf.HadoopConf
	Newconf hadoopconf.HadoopConf
	Errs map[string]error
}

func (r *ReconfigOnline) GetStatus() *ReconfigEvent{
	r.RLock()
	defer r.RUnlock()
	newconf := hadoopconf.HadoopConf{}
	for k, _ := range r.oldconf {
		newconf[k] = r.conf[k]
	}
	errs := make(map[string]error)
	for k, _ := range r.oldconf {
		errs[k] = nil
	}
	return &ReconfigEvent {
		Start: r.start,
		End: r.end,
		Oldconf: r.oldconf,
		Newconf: newconf,
		Errs: errs,
	}
}

func (r *ReconfigOnline) ListProperties() []string {
	r.RLock()
	defer r.RUnlock()
	res := make([]string, 0, len(r.conf))
	for k, _ := range r.conf {
		res = append(res, k)
	}
	return res
}

func (r *ReconfigOnline) StartUpdateConf(conf hadoopconf.HadoopConf) error {
	r.Lock()
	defer r.Unlock()
	r.start = time.Now()
	r.oldconf = r.conf.DiffValue(conf)
	for k, _ := range r.oldconf {
		r.conf[k] = conf[k]
	}
	r.end = time.Now()

	return nil
}

func NewReconfig(conf hadoopconf.HadoopConf) *ReconfigOnline {
	return &ReconfigOnline {
		RWMutex: &sync.RWMutex{},
		start: time.Unix(0, 0),
		end: time.Unix(0, 0),
		conf: conf.Clone(),
	}
}
