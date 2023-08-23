package cmd

import (
	"context"
	"log"
	"os/exec"
	"path"
	"strconv"
	"strings"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func setOwnerDec(b []byte) (proto.Message, error) {
	req := new(hdfs.SetOwnerRequestProto)
	return parseRequest(b, req)
}

func setOwner(ctx context.Context, m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.SetOwnerRequestProto)
	log.Printf("src %v\nusername %v\ngroupname %v\n", req.GetSrc(), req.GetUsername(), req.GetGroupname())
	return opfsSetOwner(req)
}

const (
	OpfsCmdDir = "/usr/libexec/openfs"
	OpfsUid = "openfs-get-uid"
	OpfsGid = "openfs-get-gid"
)

func opfsGetUid(user string) (uid int, err error) {
	uid, err = strconv.Atoi(user)
	if err == nil {
		return uid, nil
	}
	cmdPath := path.Join(OpfsCmdDir, OpfsUid)
	c := exec.Command(cmdPath, user)
	output, err := c.CombinedOutput()
	if err != nil {
		log.Printf("fail to get user uid %v, %v", user, err)
		return -1, err
	}
	uid, err = strconv.Atoi(strings.TrimSuffix(string(output), "\n"))
	if err != nil {
		log.Printf("fail to transfer output to uid %v, %v", string(output), err)
		return -1, err
	}
	return uid, nil
}
func opfsGetGid(group string) (gid int, err error) {
	gid, err = strconv.Atoi(group)
	if err == nil {
		return gid, nil
	}
	cmdPath := path.Join(OpfsCmdDir, OpfsGid)
	c := exec.Command(cmdPath, group)
	output, err := c.CombinedOutput()
	if err != nil {
		log.Printf("fail to get group gid %v, %v", group, err)
		return -1, err
	}
	gid, err = strconv.Atoi(strings.TrimSuffix(string(output), "\n"))
	if err != nil {
		log.Printf("fail to transfer output to gid %v, %v", string(output), err)
		return -1, err
	}
	return gid, nil
}

func opfsSetOwner(r *hdfs.SetOwnerRequestProto) (*hdfs.SetOwnerResponseProto, error) {
	res := new(hdfs.SetOwnerResponseProto)
	src := r.GetSrc()
	user := r.GetUsername()
	group := r.GetGroupname()

	uid := 0
	gid := 0
	var err error

	if user == "" {
		uid = -1
	} else {
		uid, err = opfsGetUid(user)
		if err != nil {
			return res, err
		}
	}
	if group == "" {
		gid = -1
	} else {
		gid, err = opfsGetGid(group)
		if err != nil {
			return res, err
		}
	}
	log.Printf("set new owner %v %v", uid, gid)

	err = opfs.SetOwner(src, uid, gid)
	if err != nil {
		log.Printf("set owner fail %v\n", err)
	}
	return res, nil
}
