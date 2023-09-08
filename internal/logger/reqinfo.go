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

package logger

import (
	"context"
	"fmt"
	"sync"
)

// Key used for Get/SetReqInfo
type contextKeyType string

const contextKey = contextKeyType("openfsHdfsContext")

// KeyVal - appended to ReqInfo.Tags
type KeyVal struct {
	Key string
	Val interface{}
}

// ReqInfo stores the request info.
type ReqInfo struct {
	RemoteHost   string          // Client Host/IP
	Host         string          // Node Host/IP
	ClientID     string          // client id
	CallID       string          // Seqid
	Method       string          // Rpc Method getLocalBlock etc
	params       []KeyVal        // method params
	User         string          // Access user name
	tags         []KeyVal        // Any additional info not accommodated by above fields
	sync.RWMutex
}

// NewReqInfo :
func NewReqInfo(remoteHost, callid, method, user string) *ReqInfo {
	return &ReqInfo{
		RemoteHost:   remoteHost,
		Method:       method,
		CallID:       callid,
		User:         user,
	}
}

// AppendTags - appends key/val to ReqInfo.tags
func (r *ReqInfo) AppendTags(key string, val interface{}) *ReqInfo {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()
	r.tags = append(r.tags, KeyVal{key, val})
	return r
}

// SetTags - sets key/val to ReqInfo.tags
func (r *ReqInfo) SetTags(key string, val interface{}) *ReqInfo {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()
	// Search of tag key already exists in tags
	var updated bool
	for _, tag := range r.tags {
		if tag.Key == key {
			tag.Val = val
			updated = true
			break
		}
	}
	if !updated {
		// Append to the end of tags list
		r.tags = append(r.tags, KeyVal{key, val})
	}
	return r
}

// GetTags - returns the user defined tags
func (r *ReqInfo) GetTags() []KeyVal {
	if r == nil {
		return nil
	}
	r.RLock()
	defer r.RUnlock()
	return append([]KeyVal(nil), r.tags...)
}

// GetTagsMap - returns the user defined tags in a map structure
func (r *ReqInfo) GetTagsMap() map[string]interface{} {
	if r == nil {
		return nil
	}
	r.RLock()
	defer r.RUnlock()
	m := make(map[string]interface{}, len(r.tags))
	for _, t := range r.tags {
		m[t.Key] = t.Val
	}
	return m
}

// AppendParams - appends key/val to ReqInfo.params
func (r *ReqInfo) AppendParams(key string, val interface{}) *ReqInfo {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()
	r.params = append(r.params, KeyVal{key, val})
	return r
}

// SetParams - sets key/val to ReqInfo.params
func (r *ReqInfo) SetParams(key string, val interface{}) *ReqInfo {
	if r == nil {
		return nil
	}
	r.Lock()
	defer r.Unlock()
	// Search of tag key already exists in tags
	var updated bool
	for i, param := range r.params {
		if param.Key == key {
			r.params = append(r.params[:i], r.params[i+1:]...)
			break
		}
	}
	if !updated {
		// Append to the end of tags list
		r.params = append(r.params, KeyVal{key, val})
	}
	return r
}

// GetParams - returns the user defined params
func (r *ReqInfo) GetParams() []KeyVal {
	if r == nil {
		return nil
	}
	r.RLock()
	defer r.RUnlock()
	return append([]KeyVal(nil), r.params...)
}

// GetParamsMap - returns the user defined tags in a map structure
func (r *ReqInfo) GetParamsMap() map[string]interface{} {
	if r == nil {
		return nil
	}
	r.RLock()
	defer r.RUnlock()
	m := make(map[string]interface{}, len(r.tags))
	for _, t := range r.params {
		m[t.Key] = t.Val
	}
	return m
}

// SetReqInfo sets ReqInfo in the context.
func SetReqInfo(ctx context.Context, req *ReqInfo) context.Context {
	if ctx == nil {
		LogIf(context.Background(), fmt.Errorf("context is nil"))
		return nil
	}
	return context.WithValue(ctx, contextKey, req)
}

// GetReqInfo returns ReqInfo if set.
func GetReqInfo(ctx context.Context) *ReqInfo {
	if ctx != nil {
		r, ok := ctx.Value(contextKey).(*ReqInfo)
		if ok {
			return r
		}
		r = &ReqInfo{}
		SetReqInfo(ctx, r)
		return r
	}
	return nil
}

func GetReqMethod(ctx context.Context) string {
	if ctx != nil {
		r := GetReqInfo(ctx)
		return r.Method
	}
	return ""
}
