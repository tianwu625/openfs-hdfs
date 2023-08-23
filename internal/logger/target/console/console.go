// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package console

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/openfs/openfs-hdfs/internal/logger"
	"github.com/openfs/openfs-hdfs/internal/logger/message/log"
	"github.com/openfs/openfs-hdfs/internal/logger/target/types"
)

// Target implements loggerTarget to send log
// in plain or json format to the standard output.
type Target struct{}

// Validate - validate if the tty can be written to
func (c *Target) Validate() error {
	return nil
}

// Endpoint returns the backend endpoint
func (c *Target) Endpoint() string {
	return ""
}

func (c *Target) String() string {
	return "console"
}

func (c *Target) Init() error {
	return nil
}

func (c *Target) Cancel() {
}

func (c *Target) Type() types.TargetType {
	return types.TargetConsole
}

// Send log message 'e' to console
func (c *Target) Send(e interface{}, logKind string) error {
	entry, ok := e.(log.Entry)
	if !ok {
		return fmt.Errorf("Uexpected log entry structure %#v", e)
	}
	if logger.IsJSON() {
		logJSON, err := json.Marshal(&entry)
		if err != nil {
			return err
		}
		fmt.Println(string(logJSON))
		return nil
	}

	traceLength := len(entry.Trace.Source)
	trace := make([]string, traceLength)

	// Add a sequence number and formatting for each stack trace
	// No formatting is required for the first entry
	for i, element := range entry.Trace.Source {
		trace[i] = fmt.Sprintf("%8v: %s", traceLength-i, element)
	}

	tagString := ""
	for key, value := range entry.Trace.Variables {
		if value != "" {
			if tagString != "" {
				tagString += ", "
			}
			tagString += fmt.Sprintf("%s=%v", key, value)
		}
	}

	var methodString string
	if entry.API != nil {
		methodString = "Method: " + entry.API.Name + "("
		if len(entry.API.Args.Params) > 0 {
			paramsString := ""
			for key, value:= range entry.API.Args.Params {
				if paramsString != "" {
					paramsString += ", "
				}
				paramsString += fmt.Sprintf("%s=%v", key, value)
			}
			methodString += "Params: " + paramsString

		}
		methodString += ")"
	} else {
		methodString = "INTERNAL"
	}
	timeString := "Time: " + entry.Time.Format(logger.TimeFormat)

	var clientID string
	if entry.ClientID != "" {
		clientID = "\nClientID: " + entry.ClientID
	}

	var callID string
	if entry.CallID != "" {
		callID = "\nCallID: " + entry.CallID
	}

	var remoteHost string
	if entry.RemoteHost != "" {
		remoteHost = "\nRemoteHost: " + entry.RemoteHost
	}

	var host string
	if entry.Host != "" {
		host = "\nHost: " + entry.Host
	}

	if len(entry.Trace.Variables) > 0 {
		tagString = "\n       " + tagString
	}

	msg := entry.Trace.Message
	output := fmt.Sprintf("\n%s\n%s%s%s%s%s\nError: %s%s\n%s",
		methodString, timeString, clientID, callID, remoteHost, host,
		msg, tagString, strings.Join(trace, "\n"))

	fmt.Println(output)
	return nil
}

// New initializes a new logger target
// which prints log directly in the standard
// output.
func New() *Target {
	return &Target{}
}
