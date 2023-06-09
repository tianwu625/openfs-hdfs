PWD=$(shell pwd)
PROTO_COMMON=$(PWD)/internal/protocol/hadoop_common
PROTO_HDFS=$(PWD)/internal/protocol/hadoop_hdfs
APP=openfs-hdfs
all: build

build:
	go build -o $(PWD)/$(APP)

proto-common:
	protoc --go_out=$(PROTO_COMMON)/ -I $(PROTO_COMMON)/ --go_opt=paths=source_relative $(PROTO_COMMON)/*.proto

proto-hdfs:
	protoc --go_out=$(PROTO_HDFS)/ -I $(PROTO_COMMON)/ -I $(PROTO_HDFS)/ --go_opt=paths=source_relative $(PROTO_HDFS)/*.proto

release: proto-common proto-hdfs build

clean:
	rm -f $(PWD)/$(APP)
clean-proto:
	rm -f $(PROTO_COMMON)/*.pb.go
	rm -f $(PROTO_HDFS)/*.pb.go
clean-all: clean clean-proto
