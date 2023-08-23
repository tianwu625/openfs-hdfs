package rpc

import (
	"fmt"

	hadoop "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_common"
)

var ErrNoAvailableServer error = fmt.Errorf("no available rpc server")
var ErrUserClose error = fmt.Errorf("user close this connector")

type RpcError struct {
	method string
	message string
	code int
	exception string
}

func (e *RpcError) Desc() string {
	return hadoop.RpcResponseHeaderProto_RpcErrorCodeProto_name[int32(e.code)]
}

func (e *RpcError) Error() string {
	s := fmt.Sprintf("%s call failed with %s", e.method, e.Desc())
	if e.exception != "" {
		s += fmt.Sprintf(" (%s)", e.exception)
	}

	return s
}

