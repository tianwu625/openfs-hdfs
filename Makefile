PWD=$(shell pwd)
PROTO_COMMON=$(PWD)/internal/protocol/hadoop_common
PROTO_HDFS=$(PWD)/internal/protocol/hadoop_hdfs
PROTO_SERVER=$(PWD)/internal/protocol/hadoop_server
PROTO_SECOND=$(PWD)/internal/protocol/hadoop_second
LDFLAGS := $(shell go run buildscripts/gen-ldflags.go)
APP=openfs-hdfs
all: build

build:
	go build --ldflags "$(LDFLAGS)" -o $(PWD)/$(APP)

proto-common:
	protoc --go_out=$(PROTO_COMMON)/ -I $(PROTO_COMMON)/ --go_opt=paths=source_relative $(PROTO_COMMON)/*.proto

proto-hdfs:
	protoc --go_out=$(PROTO_HDFS)/ -I $(PROTO_COMMON)/ -I $(PROTO_HDFS)/ --go_opt=paths=source_relative $(PROTO_HDFS)/*.proto

proto-server:
	protoc --go_out=$(PROTO_SERVER)/ -I $(PROTO_COMMON)/ -I $(PROTO_HDFS)/ -I $(PROTO_SERVER)/ --go_opt=paths=source_relative $(PROTO_SERVER)/*.proto

proto-second:
	protoc --go_out=$(PROTO_SECOND)/ -I $(PROTO_COMMON)/ -I $(PROTO_HDFS)/ -I $(PROTO_SECOND)/ --go_opt=paths=source_relative $(PROTO_SECOND)/*.proto

release: proto-common proto-hdfs proto-server proto-second build

clean:
	rm -f $(PWD)/$(APP)

clean-proto:
	rm -f $(PROTO_COMMON)/*.pb.go
	rm -f $(PROTO_HDFS)/*.pb.go
	rm -f $(PROTO_SERVER)/*.pb.go
	rm -f $(PROTO_SECOND)/*.pb.go

clean-all: clean clean-proto
