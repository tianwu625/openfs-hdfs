// Copyright (c) 2015-2022 MinIO, Inc.
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

package logger

import (
	"sync"

	"github.com/openfs/openfs-hdfs/internal/logger/target/types"
)

// Target is the entity that we will receive
// a single log entry and Send it to the log target
//   e.g. Send the log to a http server
type Target interface {
	String() string
	Endpoint() string
	Init() error
	Cancel()
	Send(entry interface{}, errKind string) error
	Type() types.TargetType
}

var (
	swapSystemMuRW sync.RWMutex

	// systemTargets is the set of enabled loggers.
	// Must be immutable at all times.
	// Can be swapped to another while holding swapMu
	systemTargets = []Target{}

	// This is always set represent /dev/console target
	consoleTgt Target
)

// SystemTargets returns active targets.
// Returned slice may not be modified in any way.
func SystemTargets() []Target {
	swapSystemMuRW.RLock()
	defer swapSystemMuRW.RUnlock()

	res := systemTargets
	return res
}

// AddSystemTarget adds a new logger target to the
// list of enabled loggers
func AddSystemTarget(t Target) error {
	if err := t.Init(); err != nil {
		return err
	}

	swapSystemMuRW.Lock()
	defer swapSystemMuRW.Unlock()

	if consoleTgt == nil {
		if t.Type() == types.TargetConsole {
			consoleTgt = t
		}
	}
	updated := append(make([]Target, 0, len(systemTargets)+1), systemTargets...)
	updated = append(updated, t)
	systemTargets = updated

	return nil
}

// Split targets into two groups:
//  group1 contains all targets of type t
//  group2 contains the remaining targets
func splitTargets(targets []Target, t types.TargetType) (group1 []Target, group2 []Target) {
	for _, target := range targets {
		if target.Type() == t {
			group1 = append(group1, target)
		} else {
			group2 = append(group2, target)
		}
	}
	return
}

func cancelTargets(targets []Target) {
	for _, target := range targets {
		target.Cancel()
	}
}
