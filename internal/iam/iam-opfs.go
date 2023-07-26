package iam

import (
	"os/exec"
	"context"
	"log"
	"os"

	jsoniter "github.com/json-iterator/go"
)

const (
	OpfsCmdDir              = "/usr/libexec/openfs"
	OpfsUserCmd             = "/openfs-auth-user-json"
	OpfsGroupCmd            = "/openfs-auth-group-json"
)

type OpfsGroupId struct {
	Gid  int    `json:"gid"`
	Name string `json:"name"`
}

type OpfsUser struct {
	Name    string        `json:"name"`
	Buildin string        `json:"builtin"`
	Groups  []OpfsGroupId `json:"groups, omitempty"`
	Pgroup  OpfsGroupId   `json:"pgroup"`
	Utype   string        `json:"type"`
	Uid     int           `json:"uid"`
	Egroup  OpfsGroupId   `json:"egroup"`
}

type opfsUserInfo struct {
	uname string
	uid int
	groups []string
}

func loadOpfsUsers(ctx context.Context, cmdPath string) ([]OpfsUser, error) {
	c := exec.Command(cmdPath)

	output, err := c.CombinedOutput()
	if err != nil {
		log.Printf("run cmd %v fail %v", cmdPath, err)
		return []OpfsUser{}, err
	}

	json := jsoniter.ConfigCompatibleWithStandardLibrary
	res := make([]OpfsUser, 0)
	json.Unmarshal(output, &res)

	return res, nil
}

func opfsLoadUser(ctx context.Context, user string) (*opfsUserInfo, error) {
	cmdPath := OpfsCmdDir + OpfsUserCmd
	res, err := loadOpfsUsers(ctx, cmdPath)
	if err != nil {
		return nil, err
	}

	for _, u := range res {
		if u.Name == user {
			uinfo := &opfsUserInfo {
				uname: u.Name,
				uid: u.Uid,
				// add pgroup
				groups: make([]string, 0, len(u.Groups) + 1),
			}
			for _, g := range u.Groups {
				uinfo.groups = append(uinfo.groups, g.Name)
			}
			uinfo.groups = append(uinfo.groups, u.Pgroup.Name)
			return uinfo, nil
		}
	}

	return nil, os.ErrNotExist
}

func opfsLoadUsers(ctx context.Context) ([]*opfsUserInfo, error) {
	cmdPath := OpfsCmdDir + OpfsUserCmd
	users, err := loadOpfsUsers(ctx, cmdPath)
	if err != nil {
		return []*opfsUserInfo{}, err
	}
	res := make([]*opfsUserInfo, 0, len(users))
	for _, u := range users {
		r := &opfsUserInfo {
			uname: u.Name,
			uid: u.Uid,
			groups: make([]string, 0, len(u.Groups) + 1),
		}
		for _, g := range u.Groups {
			r.groups = append(r.groups, g.Name)
		}
		r.groups = append(r.groups, u.Pgroup.Name)
		res = append(res, r)
	}

	return res, nil
}

type OpfsUserId struct {
	Uid  int    `json:"uid"`
	Name string `json:"name"`
}

type OpfsGroup struct {
	Name    string       `json:"name"`
	Buildin string       `json:"builtin"`
	Gtype   string       `json:"type"`
	Gid     int          `json:"gid"`
	Users   []OpfsUserId `json:"users,omitempty"`
}

func loadOPFSGroups(ctx context.Context, cmdPath string) ([]OpfsGroup, error) {
	c := exec.Command(cmdPath)

	output, err := c.CombinedOutput()
	if err != nil {
		return []OpfsGroup{}, err
	}

	json := jsoniter.ConfigCompatibleWithStandardLibrary

	res := make([]OpfsGroup, 0)
	json.Unmarshal(output, &res)

	return res, nil
}

func opfsLoadGroups(ctx context.Context) ([]string, error) {
	cmdPath := OpfsCmdDir + OpfsGroupCmd

	groups, err := loadOPFSGroups(ctx, cmdPath)
	if err != nil {
		return []string{}, err
	}
	res := make([]string, 0, len(groups))
	for _, g := range groups {
		res = append(res, g.Name)
	}
	return res, nil
}
