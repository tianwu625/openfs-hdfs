//*
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//*
// These .proto interfaces are private and stable.
// Please see https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/Compatibility.html
// for what changes are allowed for a *stable* .proto interface.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.30.0
// 	protoc        v4.23.2
// source: DatanodeLifelineProtocol.proto

package hadoop_server

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Unlike heartbeats, the response is empty. There is no command dispatch.
type LifelineResponseProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *LifelineResponseProto) Reset() {
	*x = LifelineResponseProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_DatanodeLifelineProtocol_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *LifelineResponseProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*LifelineResponseProto) ProtoMessage() {}

func (x *LifelineResponseProto) ProtoReflect() protoreflect.Message {
	mi := &file_DatanodeLifelineProtocol_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use LifelineResponseProto.ProtoReflect.Descriptor instead.
func (*LifelineResponseProto) Descriptor() ([]byte, []int) {
	return file_DatanodeLifelineProtocol_proto_rawDescGZIP(), []int{0}
}

var File_DatanodeLifelineProtocol_proto protoreflect.FileDescriptor

var file_DatanodeLifelineProtocol_proto_rawDesc = []byte{
	0x0a, 0x1e, 0x44, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x4c, 0x69, 0x66, 0x65, 0x6c, 0x69,
	0x6e, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x1c, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x64, 0x61,
	0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x6c, 0x69, 0x66, 0x65, 0x6c, 0x69, 0x6e, 0x65, 0x1a, 0x16,
	0x44, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x17, 0x0a, 0x15, 0x4c, 0x69, 0x66, 0x65, 0x6c, 0x69,
	0x6e, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x32,
	0x93, 0x01, 0x0a, 0x1f, 0x44, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x4c, 0x69, 0x66, 0x65,
	0x6c, 0x69, 0x6e, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x53, 0x65, 0x72, 0x76,
	0x69, 0x63, 0x65, 0x12, 0x70, 0x0a, 0x0c, 0x73, 0x65, 0x6e, 0x64, 0x4c, 0x69, 0x66, 0x65, 0x6c,
	0x69, 0x6e, 0x65, 0x12, 0x2b, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66,
	0x73, 0x2e, 0x64, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x2e, 0x48, 0x65, 0x61, 0x72, 0x74,
	0x62, 0x65, 0x61, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f,
	0x1a, 0x33, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x64,
	0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x6c, 0x69, 0x66, 0x65, 0x6c, 0x69, 0x6e, 0x65, 0x2e,
	0x4c, 0x69, 0x66, 0x65, 0x6c, 0x69, 0x6e, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x50, 0x72, 0x6f, 0x74, 0x6f, 0x42, 0x8c, 0x01, 0x0a, 0x25, 0x6f, 0x72, 0x67, 0x2e, 0x61, 0x70,
	0x61, 0x63, 0x68, 0x65, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x42,
	0x1e, 0x44, 0x61, 0x74, 0x61, 0x6e, 0x6f, 0x64, 0x65, 0x4c, 0x69, 0x66, 0x65, 0x6c, 0x69, 0x6e,
	0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x5a,
	0x3d, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6f, 0x70, 0x65, 0x6e,
	0x66, 0x73, 0x2f, 0x6f, 0x70, 0x65, 0x6e, 0x66, 0x73, 0x2d, 0x68, 0x64, 0x66, 0x73, 0x2f, 0x69,
	0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c,
	0x2f, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x5f, 0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x88, 0x01,
	0x01, 0xa0, 0x01, 0x01,
}

var (
	file_DatanodeLifelineProtocol_proto_rawDescOnce sync.Once
	file_DatanodeLifelineProtocol_proto_rawDescData = file_DatanodeLifelineProtocol_proto_rawDesc
)

func file_DatanodeLifelineProtocol_proto_rawDescGZIP() []byte {
	file_DatanodeLifelineProtocol_proto_rawDescOnce.Do(func() {
		file_DatanodeLifelineProtocol_proto_rawDescData = protoimpl.X.CompressGZIP(file_DatanodeLifelineProtocol_proto_rawDescData)
	})
	return file_DatanodeLifelineProtocol_proto_rawDescData
}

var file_DatanodeLifelineProtocol_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_DatanodeLifelineProtocol_proto_goTypes = []interface{}{
	(*LifelineResponseProto)(nil), // 0: hadoop.hdfs.datanodelifeline.LifelineResponseProto
	(*HeartbeatRequestProto)(nil), // 1: hadoop.hdfs.datanode.HeartbeatRequestProto
}
var file_DatanodeLifelineProtocol_proto_depIdxs = []int32{
	1, // 0: hadoop.hdfs.datanodelifeline.DatanodeLifelineProtocolService.sendLifeline:input_type -> hadoop.hdfs.datanode.HeartbeatRequestProto
	0, // 1: hadoop.hdfs.datanodelifeline.DatanodeLifelineProtocolService.sendLifeline:output_type -> hadoop.hdfs.datanodelifeline.LifelineResponseProto
	1, // [1:2] is the sub-list for method output_type
	0, // [0:1] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_DatanodeLifelineProtocol_proto_init() }
func file_DatanodeLifelineProtocol_proto_init() {
	if File_DatanodeLifelineProtocol_proto != nil {
		return
	}
	file_DatanodeProtocol_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_DatanodeLifelineProtocol_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*LifelineResponseProto); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_DatanodeLifelineProtocol_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_DatanodeLifelineProtocol_proto_goTypes,
		DependencyIndexes: file_DatanodeLifelineProtocol_proto_depIdxs,
		MessageInfos:      file_DatanodeLifelineProtocol_proto_msgTypes,
	}.Build()
	File_DatanodeLifelineProtocol_proto = out.File
	file_DatanodeLifelineProtocol_proto_rawDesc = nil
	file_DatanodeLifelineProtocol_proto_goTypes = nil
	file_DatanodeLifelineProtocol_proto_depIdxs = nil
}
