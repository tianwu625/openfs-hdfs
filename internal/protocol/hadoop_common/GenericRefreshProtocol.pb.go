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
// Please see http://wiki.apache.org/hadoop/Compatibility
// for what changes are allowed for a *stable* .proto interface.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.30.0
// 	protoc        v4.23.2
// source: GenericRefreshProtocol.proto

package hadoop_common

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

//*
//  Refresh request.
type GenericRefreshRequestProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Identifier *string  `protobuf:"bytes,1,opt,name=identifier" json:"identifier,omitempty"`
	Args       []string `protobuf:"bytes,2,rep,name=args" json:"args,omitempty"`
}

func (x *GenericRefreshRequestProto) Reset() {
	*x = GenericRefreshRequestProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_GenericRefreshProtocol_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GenericRefreshRequestProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GenericRefreshRequestProto) ProtoMessage() {}

func (x *GenericRefreshRequestProto) ProtoReflect() protoreflect.Message {
	mi := &file_GenericRefreshProtocol_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GenericRefreshRequestProto.ProtoReflect.Descriptor instead.
func (*GenericRefreshRequestProto) Descriptor() ([]byte, []int) {
	return file_GenericRefreshProtocol_proto_rawDescGZIP(), []int{0}
}

func (x *GenericRefreshRequestProto) GetIdentifier() string {
	if x != nil && x.Identifier != nil {
		return *x.Identifier
	}
	return ""
}

func (x *GenericRefreshRequestProto) GetArgs() []string {
	if x != nil {
		return x.Args
	}
	return nil
}

//*
// A single response from a refresh handler.
type GenericRefreshResponseProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	ExitStatus  *int32  `protobuf:"varint,1,opt,name=exitStatus" json:"exitStatus,omitempty"`  // unix exit status to return
	UserMessage *string `protobuf:"bytes,2,opt,name=userMessage" json:"userMessage,omitempty"` // to be displayed to the user
	SenderName  *string `protobuf:"bytes,3,opt,name=senderName" json:"senderName,omitempty"`   // which handler sent this message
}

func (x *GenericRefreshResponseProto) Reset() {
	*x = GenericRefreshResponseProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_GenericRefreshProtocol_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GenericRefreshResponseProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GenericRefreshResponseProto) ProtoMessage() {}

func (x *GenericRefreshResponseProto) ProtoReflect() protoreflect.Message {
	mi := &file_GenericRefreshProtocol_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GenericRefreshResponseProto.ProtoReflect.Descriptor instead.
func (*GenericRefreshResponseProto) Descriptor() ([]byte, []int) {
	return file_GenericRefreshProtocol_proto_rawDescGZIP(), []int{1}
}

func (x *GenericRefreshResponseProto) GetExitStatus() int32 {
	if x != nil && x.ExitStatus != nil {
		return *x.ExitStatus
	}
	return 0
}

func (x *GenericRefreshResponseProto) GetUserMessage() string {
	if x != nil && x.UserMessage != nil {
		return *x.UserMessage
	}
	return ""
}

func (x *GenericRefreshResponseProto) GetSenderName() string {
	if x != nil && x.SenderName != nil {
		return *x.SenderName
	}
	return ""
}

//*
// Collection of responses from zero or more handlers.
type GenericRefreshResponseCollectionProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Responses []*GenericRefreshResponseProto `protobuf:"bytes,1,rep,name=responses" json:"responses,omitempty"`
}

func (x *GenericRefreshResponseCollectionProto) Reset() {
	*x = GenericRefreshResponseCollectionProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_GenericRefreshProtocol_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GenericRefreshResponseCollectionProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GenericRefreshResponseCollectionProto) ProtoMessage() {}

func (x *GenericRefreshResponseCollectionProto) ProtoReflect() protoreflect.Message {
	mi := &file_GenericRefreshProtocol_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GenericRefreshResponseCollectionProto.ProtoReflect.Descriptor instead.
func (*GenericRefreshResponseCollectionProto) Descriptor() ([]byte, []int) {
	return file_GenericRefreshProtocol_proto_rawDescGZIP(), []int{2}
}

func (x *GenericRefreshResponseCollectionProto) GetResponses() []*GenericRefreshResponseProto {
	if x != nil {
		return x.Responses
	}
	return nil
}

var File_GenericRefreshProtocol_proto protoreflect.FileDescriptor

var file_GenericRefreshProtocol_proto_rawDesc = []byte{
	0x0a, 0x1c, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x69, 0x63, 0x52, 0x65, 0x66, 0x72, 0x65, 0x73, 0x68,
	0x50, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0d,
	0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x22, 0x50, 0x0a,
	0x1a, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x69, 0x63, 0x52, 0x65, 0x66, 0x72, 0x65, 0x73, 0x68, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1e, 0x0a, 0x0a, 0x69,
	0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52,
	0x0a, 0x69, 0x64, 0x65, 0x6e, 0x74, 0x69, 0x66, 0x69, 0x65, 0x72, 0x12, 0x12, 0x0a, 0x04, 0x61,
	0x72, 0x67, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x09, 0x52, 0x04, 0x61, 0x72, 0x67, 0x73, 0x22,
	0x7f, 0x0a, 0x1b, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x69, 0x63, 0x52, 0x65, 0x66, 0x72, 0x65, 0x73,
	0x68, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1e,
	0x0a, 0x0a, 0x65, 0x78, 0x69, 0x74, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x05, 0x52, 0x0a, 0x65, 0x78, 0x69, 0x74, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x20,
	0x0a, 0x0b, 0x75, 0x73, 0x65, 0x72, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0b, 0x75, 0x73, 0x65, 0x72, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
	0x12, 0x1e, 0x0a, 0x0a, 0x73, 0x65, 0x6e, 0x64, 0x65, 0x72, 0x4e, 0x61, 0x6d, 0x65, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x73, 0x65, 0x6e, 0x64, 0x65, 0x72, 0x4e, 0x61, 0x6d, 0x65,
	0x22, 0x71, 0x0a, 0x25, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x69, 0x63, 0x52, 0x65, 0x66, 0x72, 0x65,
	0x73, 0x68, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x43, 0x6f, 0x6c, 0x6c, 0x65, 0x63,
	0x74, 0x69, 0x6f, 0x6e, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x48, 0x0a, 0x09, 0x72, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2a, 0x2e, 0x68,
	0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x47, 0x65, 0x6e,
	0x65, 0x72, 0x69, 0x63, 0x52, 0x65, 0x66, 0x72, 0x65, 0x73, 0x68, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x52, 0x09, 0x72, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x73, 0x32, 0x8b, 0x01, 0x0a, 0x1d, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x69, 0x63, 0x52,
	0x65, 0x66, 0x72, 0x65, 0x73, 0x68, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x53, 0x65,
	0x72, 0x76, 0x69, 0x63, 0x65, 0x12, 0x6a, 0x0a, 0x07, 0x72, 0x65, 0x66, 0x72, 0x65, 0x73, 0x68,
	0x12, 0x29, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e,
	0x2e, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x69, 0x63, 0x52, 0x65, 0x66, 0x72, 0x65, 0x73, 0x68, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x34, 0x2e, 0x68, 0x61,
	0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2e, 0x47, 0x65, 0x6e, 0x65,
	0x72, 0x69, 0x63, 0x52, 0x65, 0x66, 0x72, 0x65, 0x73, 0x68, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x43, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x50, 0x72, 0x6f, 0x74,
	0x6f, 0x42, 0x80, 0x01, 0x0a, 0x1b, 0x6f, 0x72, 0x67, 0x2e, 0x61, 0x70, 0x61, 0x63, 0x68, 0x65,
	0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x69, 0x70, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x42, 0x1c, 0x47, 0x65, 0x6e, 0x65, 0x72, 0x69, 0x63, 0x52, 0x65, 0x66, 0x72, 0x65, 0x73,
	0x68, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x5a,
	0x3d, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x6f, 0x70, 0x65, 0x6e,
	0x66, 0x73, 0x2f, 0x6f, 0x70, 0x65, 0x6e, 0x66, 0x73, 0x2d, 0x68, 0x64, 0x66, 0x73, 0x2f, 0x69,
	0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c,
	0x2f, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x5f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x88, 0x01,
	0x01, 0xa0, 0x01, 0x01,
}

var (
	file_GenericRefreshProtocol_proto_rawDescOnce sync.Once
	file_GenericRefreshProtocol_proto_rawDescData = file_GenericRefreshProtocol_proto_rawDesc
)

func file_GenericRefreshProtocol_proto_rawDescGZIP() []byte {
	file_GenericRefreshProtocol_proto_rawDescOnce.Do(func() {
		file_GenericRefreshProtocol_proto_rawDescData = protoimpl.X.CompressGZIP(file_GenericRefreshProtocol_proto_rawDescData)
	})
	return file_GenericRefreshProtocol_proto_rawDescData
}

var file_GenericRefreshProtocol_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_GenericRefreshProtocol_proto_goTypes = []interface{}{
	(*GenericRefreshRequestProto)(nil),            // 0: hadoop.common.GenericRefreshRequestProto
	(*GenericRefreshResponseProto)(nil),           // 1: hadoop.common.GenericRefreshResponseProto
	(*GenericRefreshResponseCollectionProto)(nil), // 2: hadoop.common.GenericRefreshResponseCollectionProto
}
var file_GenericRefreshProtocol_proto_depIdxs = []int32{
	1, // 0: hadoop.common.GenericRefreshResponseCollectionProto.responses:type_name -> hadoop.common.GenericRefreshResponseProto
	0, // 1: hadoop.common.GenericRefreshProtocolService.refresh:input_type -> hadoop.common.GenericRefreshRequestProto
	2, // 2: hadoop.common.GenericRefreshProtocolService.refresh:output_type -> hadoop.common.GenericRefreshResponseCollectionProto
	2, // [2:3] is the sub-list for method output_type
	1, // [1:2] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_GenericRefreshProtocol_proto_init() }
func file_GenericRefreshProtocol_proto_init() {
	if File_GenericRefreshProtocol_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_GenericRefreshProtocol_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GenericRefreshRequestProto); i {
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
		file_GenericRefreshProtocol_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GenericRefreshResponseProto); i {
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
		file_GenericRefreshProtocol_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GenericRefreshResponseCollectionProto); i {
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
			RawDescriptor: file_GenericRefreshProtocol_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_GenericRefreshProtocol_proto_goTypes,
		DependencyIndexes: file_GenericRefreshProtocol_proto_depIdxs,
		MessageInfos:      file_GenericRefreshProtocol_proto_msgTypes,
	}.Build()
	File_GenericRefreshProtocol_proto = out.File
	file_GenericRefreshProtocol_proto_rawDesc = nil
	file_GenericRefreshProtocol_proto_goTypes = nil
	file_GenericRefreshProtocol_proto_depIdxs = nil
}
