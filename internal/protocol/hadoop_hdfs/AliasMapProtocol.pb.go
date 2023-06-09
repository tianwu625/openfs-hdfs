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

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.30.0
// 	protoc        v4.23.2
// source: AliasMapProtocol.proto

package hadoop_hdfs

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

type KeyValueProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   *BlockProto                   `protobuf:"bytes,1,opt,name=key" json:"key,omitempty"`
	Value *ProvidedStorageLocationProto `protobuf:"bytes,2,opt,name=value" json:"value,omitempty"`
}

func (x *KeyValueProto) Reset() {
	*x = KeyValueProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_AliasMapProtocol_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *KeyValueProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*KeyValueProto) ProtoMessage() {}

func (x *KeyValueProto) ProtoReflect() protoreflect.Message {
	mi := &file_AliasMapProtocol_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use KeyValueProto.ProtoReflect.Descriptor instead.
func (*KeyValueProto) Descriptor() ([]byte, []int) {
	return file_AliasMapProtocol_proto_rawDescGZIP(), []int{0}
}

func (x *KeyValueProto) GetKey() *BlockProto {
	if x != nil {
		return x.Key
	}
	return nil
}

func (x *KeyValueProto) GetValue() *ProvidedStorageLocationProto {
	if x != nil {
		return x.Value
	}
	return nil
}

type WriteRequestProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	KeyValuePair *KeyValueProto `protobuf:"bytes,1,req,name=keyValuePair" json:"keyValuePair,omitempty"`
}

func (x *WriteRequestProto) Reset() {
	*x = WriteRequestProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_AliasMapProtocol_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteRequestProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteRequestProto) ProtoMessage() {}

func (x *WriteRequestProto) ProtoReflect() protoreflect.Message {
	mi := &file_AliasMapProtocol_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteRequestProto.ProtoReflect.Descriptor instead.
func (*WriteRequestProto) Descriptor() ([]byte, []int) {
	return file_AliasMapProtocol_proto_rawDescGZIP(), []int{1}
}

func (x *WriteRequestProto) GetKeyValuePair() *KeyValueProto {
	if x != nil {
		return x.KeyValuePair
	}
	return nil
}

type WriteResponseProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *WriteResponseProto) Reset() {
	*x = WriteResponseProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_AliasMapProtocol_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *WriteResponseProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*WriteResponseProto) ProtoMessage() {}

func (x *WriteResponseProto) ProtoReflect() protoreflect.Message {
	mi := &file_AliasMapProtocol_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use WriteResponseProto.ProtoReflect.Descriptor instead.
func (*WriteResponseProto) Descriptor() ([]byte, []int) {
	return file_AliasMapProtocol_proto_rawDescGZIP(), []int{2}
}

type ReadRequestProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key *BlockProto `protobuf:"bytes,1,req,name=key" json:"key,omitempty"`
}

func (x *ReadRequestProto) Reset() {
	*x = ReadRequestProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_AliasMapProtocol_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReadRequestProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReadRequestProto) ProtoMessage() {}

func (x *ReadRequestProto) ProtoReflect() protoreflect.Message {
	mi := &file_AliasMapProtocol_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReadRequestProto.ProtoReflect.Descriptor instead.
func (*ReadRequestProto) Descriptor() ([]byte, []int) {
	return file_AliasMapProtocol_proto_rawDescGZIP(), []int{3}
}

func (x *ReadRequestProto) GetKey() *BlockProto {
	if x != nil {
		return x.Key
	}
	return nil
}

type ReadResponseProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value *ProvidedStorageLocationProto `protobuf:"bytes,1,opt,name=value" json:"value,omitempty"`
}

func (x *ReadResponseProto) Reset() {
	*x = ReadResponseProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_AliasMapProtocol_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ReadResponseProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ReadResponseProto) ProtoMessage() {}

func (x *ReadResponseProto) ProtoReflect() protoreflect.Message {
	mi := &file_AliasMapProtocol_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ReadResponseProto.ProtoReflect.Descriptor instead.
func (*ReadResponseProto) Descriptor() ([]byte, []int) {
	return file_AliasMapProtocol_proto_rawDescGZIP(), []int{4}
}

func (x *ReadResponseProto) GetValue() *ProvidedStorageLocationProto {
	if x != nil {
		return x.Value
	}
	return nil
}

type ListRequestProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Marker *BlockProto `protobuf:"bytes,1,opt,name=marker" json:"marker,omitempty"`
}

func (x *ListRequestProto) Reset() {
	*x = ListRequestProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_AliasMapProtocol_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListRequestProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListRequestProto) ProtoMessage() {}

func (x *ListRequestProto) ProtoReflect() protoreflect.Message {
	mi := &file_AliasMapProtocol_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListRequestProto.ProtoReflect.Descriptor instead.
func (*ListRequestProto) Descriptor() ([]byte, []int) {
	return file_AliasMapProtocol_proto_rawDescGZIP(), []int{5}
}

func (x *ListRequestProto) GetMarker() *BlockProto {
	if x != nil {
		return x.Marker
	}
	return nil
}

type ListResponseProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	FileRegions []*KeyValueProto `protobuf:"bytes,1,rep,name=fileRegions" json:"fileRegions,omitempty"`
	NextMarker  *BlockProto      `protobuf:"bytes,2,opt,name=nextMarker" json:"nextMarker,omitempty"`
}

func (x *ListResponseProto) Reset() {
	*x = ListResponseProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_AliasMapProtocol_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ListResponseProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListResponseProto) ProtoMessage() {}

func (x *ListResponseProto) ProtoReflect() protoreflect.Message {
	mi := &file_AliasMapProtocol_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListResponseProto.ProtoReflect.Descriptor instead.
func (*ListResponseProto) Descriptor() ([]byte, []int) {
	return file_AliasMapProtocol_proto_rawDescGZIP(), []int{6}
}

func (x *ListResponseProto) GetFileRegions() []*KeyValueProto {
	if x != nil {
		return x.FileRegions
	}
	return nil
}

func (x *ListResponseProto) GetNextMarker() *BlockProto {
	if x != nil {
		return x.NextMarker
	}
	return nil
}

type BlockPoolRequestProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *BlockPoolRequestProto) Reset() {
	*x = BlockPoolRequestProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_AliasMapProtocol_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BlockPoolRequestProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BlockPoolRequestProto) ProtoMessage() {}

func (x *BlockPoolRequestProto) ProtoReflect() protoreflect.Message {
	mi := &file_AliasMapProtocol_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BlockPoolRequestProto.ProtoReflect.Descriptor instead.
func (*BlockPoolRequestProto) Descriptor() ([]byte, []int) {
	return file_AliasMapProtocol_proto_rawDescGZIP(), []int{7}
}

type BlockPoolResponseProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	BlockPoolId *string `protobuf:"bytes,1,req,name=blockPoolId" json:"blockPoolId,omitempty"`
}

func (x *BlockPoolResponseProto) Reset() {
	*x = BlockPoolResponseProto{}
	if protoimpl.UnsafeEnabled {
		mi := &file_AliasMapProtocol_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BlockPoolResponseProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BlockPoolResponseProto) ProtoMessage() {}

func (x *BlockPoolResponseProto) ProtoReflect() protoreflect.Message {
	mi := &file_AliasMapProtocol_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BlockPoolResponseProto.ProtoReflect.Descriptor instead.
func (*BlockPoolResponseProto) Descriptor() ([]byte, []int) {
	return file_AliasMapProtocol_proto_rawDescGZIP(), []int{8}
}

func (x *BlockPoolResponseProto) GetBlockPoolId() string {
	if x != nil && x.BlockPoolId != nil {
		return *x.BlockPoolId
	}
	return ""
}

var File_AliasMapProtocol_proto protoreflect.FileDescriptor

var file_AliasMapProtocol_proto_rawDesc = []byte{
	0x0a, 0x16, 0x41, 0x6c, 0x69, 0x61, 0x73, 0x4d, 0x61, 0x70, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x63,
	0x6f, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x0b, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70,
	0x2e, 0x68, 0x64, 0x66, 0x73, 0x1a, 0x0a, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x22, 0x7b, 0x0a, 0x0d, 0x4b, 0x65, 0x79, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x50, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x29, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x17, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x42, 0x6c,
	0x6f, 0x63, 0x6b, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x3f, 0x0a,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x29, 0x2e, 0x68,
	0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x50, 0x72, 0x6f, 0x76, 0x69,
	0x64, 0x65, 0x64, 0x53, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x4c, 0x6f, 0x63, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x53,
	0x0a, 0x11, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x50, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x3e, 0x0a, 0x0c, 0x6b, 0x65, 0x79, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x50,
	0x61, 0x69, 0x72, 0x18, 0x01, 0x20, 0x02, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x68, 0x61, 0x64, 0x6f,
	0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x4b, 0x65, 0x79, 0x56, 0x61, 0x6c, 0x75, 0x65,
	0x50, 0x72, 0x6f, 0x74, 0x6f, 0x52, 0x0c, 0x6b, 0x65, 0x79, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x50,
	0x61, 0x69, 0x72, 0x22, 0x14, 0x0a, 0x12, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x3d, 0x0a, 0x10, 0x52, 0x65, 0x61,
	0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x29, 0x0a,
	0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x02, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x68, 0x61, 0x64,
	0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x50, 0x72,
	0x6f, 0x74, 0x6f, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x22, 0x54, 0x0a, 0x11, 0x52, 0x65, 0x61, 0x64,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x3f, 0x0a,
	0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x29, 0x2e, 0x68,
	0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x50, 0x72, 0x6f, 0x76, 0x69,
	0x64, 0x65, 0x64, 0x53, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x4c, 0x6f, 0x63, 0x61, 0x74, 0x69,
	0x6f, 0x6e, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x22, 0x43,
	0x0a, 0x10, 0x4c, 0x69, 0x73, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x2f, 0x0a, 0x06, 0x6d, 0x61, 0x72, 0x6b, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x17, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73,
	0x2e, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x52, 0x06, 0x6d, 0x61, 0x72,
	0x6b, 0x65, 0x72, 0x22, 0x8a, 0x01, 0x0a, 0x11, 0x4c, 0x69, 0x73, 0x74, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x3c, 0x0a, 0x0b, 0x66, 0x69, 0x6c,
	0x65, 0x52, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x1a,
	0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x4b, 0x65, 0x79,
	0x56, 0x61, 0x6c, 0x75, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x52, 0x0b, 0x66, 0x69, 0x6c, 0x65,
	0x52, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x73, 0x12, 0x37, 0x0a, 0x0a, 0x6e, 0x65, 0x78, 0x74, 0x4d,
	0x61, 0x72, 0x6b, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x68, 0x61,
	0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x50,
	0x72, 0x6f, 0x74, 0x6f, 0x52, 0x0a, 0x6e, 0x65, 0x78, 0x74, 0x4d, 0x61, 0x72, 0x6b, 0x65, 0x72,
	0x22, 0x17, 0x0a, 0x15, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x50, 0x6f, 0x6f, 0x6c, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x3a, 0x0a, 0x16, 0x42, 0x6c, 0x6f,
	0x63, 0x6b, 0x50, 0x6f, 0x6f, 0x6c, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x50, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x20, 0x0a, 0x0b, 0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x50, 0x6f, 0x6f, 0x6c,
	0x49, 0x64, 0x18, 0x01, 0x20, 0x02, 0x28, 0x09, 0x52, 0x0b, 0x62, 0x6c, 0x6f, 0x63, 0x6b, 0x50,
	0x6f, 0x6f, 0x6c, 0x49, 0x64, 0x32, 0xcc, 0x02, 0x0a, 0x17, 0x41, 0x6c, 0x69, 0x61, 0x73, 0x4d,
	0x61, 0x70, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x53, 0x65, 0x72, 0x76, 0x69, 0x63,
	0x65, 0x12, 0x48, 0x0a, 0x05, 0x77, 0x72, 0x69, 0x74, 0x65, 0x12, 0x1e, 0x2e, 0x68, 0x61, 0x64,
	0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1f, 0x2e, 0x68, 0x61, 0x64,
	0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x57, 0x72, 0x69, 0x74, 0x65, 0x52, 0x65,
	0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x45, 0x0a, 0x04, 0x72,
	0x65, 0x61, 0x64, 0x12, 0x1d, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66,
	0x73, 0x2e, 0x52, 0x65, 0x61, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f,
	0x74, 0x6f, 0x1a, 0x1e, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73,
	0x2e, 0x52, 0x65, 0x61, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x50, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x45, 0x0a, 0x04, 0x6c, 0x69, 0x73, 0x74, 0x12, 0x1d, 0x2e, 0x68, 0x61, 0x64,
	0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1e, 0x2e, 0x68, 0x61, 0x64, 0x6f,
	0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x4c, 0x69, 0x73, 0x74, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x59, 0x0a, 0x0e, 0x67, 0x65, 0x74,
	0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x50, 0x6f, 0x6f, 0x6c, 0x49, 0x64, 0x12, 0x22, 0x2e, 0x68, 0x61,
	0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x42, 0x6c, 0x6f, 0x63, 0x6b, 0x50,
	0x6f, 0x6f, 0x6c, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x23, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e, 0x42, 0x6c,
	0x6f, 0x63, 0x6b, 0x50, 0x6f, 0x6f, 0x6c, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x50,
	0x72, 0x6f, 0x74, 0x6f, 0x42, 0x82, 0x01, 0x0a, 0x25, 0x6f, 0x72, 0x67, 0x2e, 0x61, 0x70, 0x61,
	0x63, 0x68, 0x65, 0x2e, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x2e, 0x68, 0x64, 0x66, 0x73, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x42, 0x16,
	0x41, 0x6c, 0x69, 0x61, 0x73, 0x4d, 0x61, 0x70, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c,
	0x50, 0x72, 0x6f, 0x74, 0x6f, 0x73, 0x5a, 0x3b, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x6f, 0x70, 0x65, 0x6e, 0x66, 0x73, 0x2f, 0x6f, 0x70, 0x65, 0x6e, 0x66, 0x73,
	0x2d, 0x68, 0x64, 0x66, 0x73, 0x2f, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x2f, 0x68, 0x61, 0x64, 0x6f, 0x6f, 0x70, 0x5f, 0x68,
	0x64, 0x66, 0x73, 0x88, 0x01, 0x01, 0xa0, 0x01, 0x01,
}

var (
	file_AliasMapProtocol_proto_rawDescOnce sync.Once
	file_AliasMapProtocol_proto_rawDescData = file_AliasMapProtocol_proto_rawDesc
)

func file_AliasMapProtocol_proto_rawDescGZIP() []byte {
	file_AliasMapProtocol_proto_rawDescOnce.Do(func() {
		file_AliasMapProtocol_proto_rawDescData = protoimpl.X.CompressGZIP(file_AliasMapProtocol_proto_rawDescData)
	})
	return file_AliasMapProtocol_proto_rawDescData
}

var file_AliasMapProtocol_proto_msgTypes = make([]protoimpl.MessageInfo, 9)
var file_AliasMapProtocol_proto_goTypes = []interface{}{
	(*KeyValueProto)(nil),                // 0: hadoop.hdfs.KeyValueProto
	(*WriteRequestProto)(nil),            // 1: hadoop.hdfs.WriteRequestProto
	(*WriteResponseProto)(nil),           // 2: hadoop.hdfs.WriteResponseProto
	(*ReadRequestProto)(nil),             // 3: hadoop.hdfs.ReadRequestProto
	(*ReadResponseProto)(nil),            // 4: hadoop.hdfs.ReadResponseProto
	(*ListRequestProto)(nil),             // 5: hadoop.hdfs.ListRequestProto
	(*ListResponseProto)(nil),            // 6: hadoop.hdfs.ListResponseProto
	(*BlockPoolRequestProto)(nil),        // 7: hadoop.hdfs.BlockPoolRequestProto
	(*BlockPoolResponseProto)(nil),       // 8: hadoop.hdfs.BlockPoolResponseProto
	(*BlockProto)(nil),                   // 9: hadoop.hdfs.BlockProto
	(*ProvidedStorageLocationProto)(nil), // 10: hadoop.hdfs.ProvidedStorageLocationProto
}
var file_AliasMapProtocol_proto_depIdxs = []int32{
	9,  // 0: hadoop.hdfs.KeyValueProto.key:type_name -> hadoop.hdfs.BlockProto
	10, // 1: hadoop.hdfs.KeyValueProto.value:type_name -> hadoop.hdfs.ProvidedStorageLocationProto
	0,  // 2: hadoop.hdfs.WriteRequestProto.keyValuePair:type_name -> hadoop.hdfs.KeyValueProto
	9,  // 3: hadoop.hdfs.ReadRequestProto.key:type_name -> hadoop.hdfs.BlockProto
	10, // 4: hadoop.hdfs.ReadResponseProto.value:type_name -> hadoop.hdfs.ProvidedStorageLocationProto
	9,  // 5: hadoop.hdfs.ListRequestProto.marker:type_name -> hadoop.hdfs.BlockProto
	0,  // 6: hadoop.hdfs.ListResponseProto.fileRegions:type_name -> hadoop.hdfs.KeyValueProto
	9,  // 7: hadoop.hdfs.ListResponseProto.nextMarker:type_name -> hadoop.hdfs.BlockProto
	1,  // 8: hadoop.hdfs.AliasMapProtocolService.write:input_type -> hadoop.hdfs.WriteRequestProto
	3,  // 9: hadoop.hdfs.AliasMapProtocolService.read:input_type -> hadoop.hdfs.ReadRequestProto
	5,  // 10: hadoop.hdfs.AliasMapProtocolService.list:input_type -> hadoop.hdfs.ListRequestProto
	7,  // 11: hadoop.hdfs.AliasMapProtocolService.getBlockPoolId:input_type -> hadoop.hdfs.BlockPoolRequestProto
	2,  // 12: hadoop.hdfs.AliasMapProtocolService.write:output_type -> hadoop.hdfs.WriteResponseProto
	4,  // 13: hadoop.hdfs.AliasMapProtocolService.read:output_type -> hadoop.hdfs.ReadResponseProto
	6,  // 14: hadoop.hdfs.AliasMapProtocolService.list:output_type -> hadoop.hdfs.ListResponseProto
	8,  // 15: hadoop.hdfs.AliasMapProtocolService.getBlockPoolId:output_type -> hadoop.hdfs.BlockPoolResponseProto
	12, // [12:16] is the sub-list for method output_type
	8,  // [8:12] is the sub-list for method input_type
	8,  // [8:8] is the sub-list for extension type_name
	8,  // [8:8] is the sub-list for extension extendee
	0,  // [0:8] is the sub-list for field type_name
}

func init() { file_AliasMapProtocol_proto_init() }
func file_AliasMapProtocol_proto_init() {
	if File_AliasMapProtocol_proto != nil {
		return
	}
	file_hdfs_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_AliasMapProtocol_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*KeyValueProto); i {
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
		file_AliasMapProtocol_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*WriteRequestProto); i {
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
		file_AliasMapProtocol_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*WriteResponseProto); i {
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
		file_AliasMapProtocol_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReadRequestProto); i {
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
		file_AliasMapProtocol_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ReadResponseProto); i {
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
		file_AliasMapProtocol_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListRequestProto); i {
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
		file_AliasMapProtocol_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ListResponseProto); i {
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
		file_AliasMapProtocol_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BlockPoolRequestProto); i {
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
		file_AliasMapProtocol_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BlockPoolResponseProto); i {
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
			RawDescriptor: file_AliasMapProtocol_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   9,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_AliasMapProtocol_proto_goTypes,
		DependencyIndexes: file_AliasMapProtocol_proto_depIdxs,
		MessageInfos:      file_AliasMapProtocol_proto_msgTypes,
	}.Build()
	File_AliasMapProtocol_proto = out.File
	file_AliasMapProtocol_proto_rawDesc = nil
	file_AliasMapProtocol_proto_goTypes = nil
	file_AliasMapProtocol_proto_depIdxs = nil
}
