package cmd

import (
	"log"
	"os/exec"
	"path"
	"strconv"

	"github.com/openfs/openfs-hdfs/internal/opfs"
	hdfs "github.com/openfs/openfs-hdfs/internal/protocol/hadoop_hdfs"
	"google.golang.org/protobuf/proto"
)

func setOwnerDec(b []byte) (proto.Message, error) {
	req := new(hdfs.SetOwnerRequestProto)
	return parseRequest(b, req)
}

func setOwner(m proto.Message) (proto.Message, error) {
	req := m.(*hdfs.SetOwnerRequestProto)
	log.Printf("src %v\nusername %v\ngroupname%v\n", req.GetSrc(), req.GetUsername(), req.GetGroupname())
	return opfsSetOwner(req)
}

const (
	OpfsCmdDir = "/usr/libexec/openfs"
	OpfsUid = "openfs-get-uid"
	OpfsGid = "openfs-get-gid"
)

func opfsGetUid(user string) (uid int, err error) {
	cmdPath := path.Join(OpfsCmdDir, OpfsUid)
	c := exec.Command(cmdPath, user)
	output, err := c.CombinedOutput()
	if err != nil {
		return -1, err
	}
	uid, _ = strconv.Atoi(string(output))
	return uid, nil
}
func opfsGetGid(group string) (gid int, err error) {
	cmdPath := path.Join(OpfsCmdDir, OpfsGid)
	c := exec.Command(cmdPath, group)
	output, err := c.CombinedOutput()
	if err != nil {
		return -1, err
	}
	gid, _ = strconv.Atoi(string(output))
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
		uid, err = strconv.Atoi(user)
		if err != nil {
			uid, err = opfsGetUid(user)
			if err != nil {
				return res, err
			}
		}
	}
	if group == "" {
		gid = -1
	} else {
		gid, err = strconv.Atoi(group)
		if err != nil {
			gid, err = opfsGetGid(group)
			if err != nil {
				return res, err
			}
		}
	}

	err = opfs.SetOwner(src, uid, gid)
	if err != nil {
		log.Printf("set owner fail %v\n", err)
	}
	return res, nil
}
