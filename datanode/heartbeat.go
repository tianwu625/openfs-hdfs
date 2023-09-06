package datanode

import (
	"sync"
	"time"
	"log"

	hdsp "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_server"
	"google.golang.org/protobuf/proto"
)

type heartbeatManager struct {
	*sync.RWMutex
	reset bool
	interval time.Duration
	fullreport bool
	client *namenodeRpc
	sys *datanodeSys
}

func (h *heartbeatManager) getCycle() time.Duration {
	h.RLock()
	defer h.RUnlock()
	return h.interval
}

func (h *heartbeatManager) getSys() *datanodeSys {
	h.RLock()
	defer h.RUnlock()
	return h.sys
}

func (h *heartbeatManager) getClient() *namenodeRpc {
	h.RLock()
	defer h.RUnlock()
	return h.client
}

func (h *heartbeatManager) getFullReport() bool {
	h.RLock()
	defer h.RUnlock()
	return h.fullreport
}

func (h *heartbeatManager) setFullReport(fullreport bool) error {
	h.Lock()
	defer h.Unlock()
	h.fullreport = fullreport
	return nil
}

func (h *heartbeatManager) processResponse(resp *hdsp.HeartbeatResponseProto) {
	sys := h.getSys()
	cmds := resp.GetCmds()
	for _, cmd := range cmds {
		log.Printf("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!cmd %v", cmd)
	}

	if h.getFullReport() && resp.GetFullBlockReportLeaseId() != 0 {
		h.setFullReport(false)
		go sys.doFullBlockReport(resp.GetFullBlockReportLeaseId())
	}
}

func (h *heartbeatManager) heartbeat() {
	sys := h.getSys()
	client := h.getClient()

	req := &hdsp.HeartbeatRequestProto{
		Registration: client.reg,
		Reports: sys.getReports(),
		XmitsInProgress: proto.Uint32(0),
		XceiverCount: proto.Uint32(0),
		FailedVolumes: proto.Uint32(0),
		RequestFullBlockReportLease: proto.Bool(h.getFullReport()),
	}
//	log.Printf("req %v", req)
	resp, err := client.sendHeartbeat(req)
	if err != nil {
		log.Printf("send heartbeat fail")
	}
//	log.Printf("resp %v", resp)
	h.processResponse(resp)
}

func (h *heartbeatManager) getReset() (time.Duration, bool) {
	h.Lock()
	defer h.Unlock()
	reset := h.reset
	if reset{
		h.reset = false
	}

	return h.interval, reset
}

func (h *heartbeatManager) setReset(cycle time.Duration) error {
	h.Lock()
	defer h.Unlock()
	h.reset = true
	h.interval = cycle
	return nil
}

func (h *heartbeatManager)start() {
	cycle := h.getCycle()
	ticker := time.NewTicker(cycle)
	defer ticker.Stop()

	for {
		<-ticker.C
		h.heartbeat()
		cycle, reset := h.getReset()
		if reset {
			ticker.Reset(cycle)
		}
	}
}

func NewHeartbeatManager(sys *datanodeSys) *heartbeatManager {
	return &heartbeatManager{
		RWMutex:&sync.RWMutex{},
		client: sys.namenode,
		sys: sys,
		interval: sys.GetHeartbeatInterval(),
	}
}
