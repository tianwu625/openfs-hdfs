package log

import (
	"strings"
	"time"
)

// Args - defines the arguments for the API.
type Args struct {
	Params  map[string]interface{} `json:"params,omitempty"`
}

// Trace - defines the trace.
type Trace struct {
	Message   string                 `json:"message,omitempty"`
	Source    []string               `json:"source,omitempty"`
	Variables map[string]interface{} `json:"variables,omitempty"`
}

// API - defines the api type and its args.
type API struct {
	Name string `json:"name,omitempty"`
	Args *Args  `json:"args,omitempty"`
}

// Entry - defines fields and values of each log entry.
type Entry struct {
	Level        string    `json:"level"`
	LogKind      string    `json:"errKind"`
	Time         time.Time `json:"time"`
	API          *API      `json:"api,omitempty"`
	RemoteHost   string    `json:"remotehost,omitempty"`
	Host         string    `json:"host,omitempty"`
	ClientID     string    `json:"clientID,omitempty"`
	CallID       string    `json:"callID,omitempty"`
	Message      string    `json:"message,omitempty"`
	Trace        *Trace    `json:"error,omitempty"`
}

// Info holds console log messages
type Info struct {
	Entry
	ConsoleMsg string
	NodeName   string `json:"node"`
	Err        error  `json:"-"`
}

// SendLog returns true if log pertains to node specified in args.
func (l Info) SendLog(node, logKind string) bool {
	nodeFltr := (node == "" || strings.EqualFold(node, l.NodeName))
	typeFltr := strings.EqualFold(logKind, "all") || strings.EqualFold(l.LogKind, logKind)
	return nodeFltr && typeFltr
}
