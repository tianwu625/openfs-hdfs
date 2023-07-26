package iam

import (
	"log"
	"sync"
	"time"
	"errors"
	"context"
	"fmt"
	"os"
)

type CacheUserInfo struct {
	Valid bool
	StayInCache time.Time
	StartUpdate time.Time
	Uid int
	Groups []string
}

type UpdateUserInfoReq struct {
	user string
	sys *IAMSys
}

type actionType int

const (
	actionQuit actionType = iota
	actionUpdateUserInfo
)

type WorkRequest struct {
	action actionType
	req interface{}
}

type IAMSys struct {
	*sync.RWMutex
	stay time.Duration
	background bool
	backthread int
	negative time.Duration
	workqueue chan WorkRequest
	users map[string]*CacheUserInfo
	waiters map[string]chan error
	rootgroups map[string]struct{}
}

type IAMSysConf struct {
	Stay time.Duration
	Drop time.Duration
	BackGround bool
	BackGroundThread int
	Negative time.Duration
	RootGroups []string
}

var errNotNeedUpdate = errors.New("no need update")

func (sys *IAMSys) ObtainChan(user string) (chan error, error) {
	sys.Lock()
	defer sys.Unlock()
	userinfo, ok := sys.users[user]
	if !ok {
		sys.users[user] = &CacheUserInfo {
			Valid: false,
			StayInCache: time.Unix(0, 0),
			StartUpdate: time.Now(),
		}
		c, ok := sys.waiters[user]
		if !ok {
			c = make(chan error)
			sys.waiters[user] = c
			return c, nil
		}
		log.Printf("need to check logic for this, because of have this channel but no user info")
		return c, nil
	}

	if userinfo.Valid && time.Since(userinfo.StayInCache) < sys.stay && userinfo.StartUpdate.Equal(time.Unix(0,0)) {
		return make(chan error), errNotNeedUpdate
	}

	c, ok := sys.waiters[user]
	if !ok {
		c = make(chan error)
		sys.waiters[user] = c
	}

	return c, nil
}

func (sys *IAMSys) loadUserWithoutLock (user string) error {
	ctx := context.Background()
	info, err := opfsLoadUser(ctx, user)
	if err != nil {
		return err
	}
	userinfo, ok := sys.users[user]
	if !ok {
		panic(fmt.Errorf("user not exist in cache?"))
	}
	userinfo.Valid = true
	userinfo.StayInCache = time.Now()
	//contain to update to over
	userinfo.StartUpdate = time.Unix(0,0)
	userinfo.Uid = info.uid
	userinfo.Groups = make([]string, len(info.groups))
	copy(userinfo.Groups, info.groups)

	return nil
}

func (sys *IAMSys) LoadUser (user string) error {
	sys.Lock()
	defer sys.Unlock()

	ctx := context.Background()
	info, err := opfsLoadUser(ctx, user)
	if err != nil {
		return err
	}
	userinfo, ok := sys.users[user]
	if !ok {
		panic(fmt.Errorf("user not exist in cache?"))
	}
	userinfo.Valid = true
	userinfo.StayInCache = time.Now()
	//contain to update to over
	userinfo.StartUpdate = time.Unix(0,0)
	userinfo.Uid = info.uid
	userinfo.Groups = make([]string, len(info.groups))
	copy(userinfo.Groups, info.groups)

	return nil
}

const (
	tenTimes = 10
)

func threadWork(c <-chan WorkRequest) {
	quit := false
	for !quit {
		req, ok := <-c
		if !ok {
			panic(fmt.Errorf("workqueue should be no close"))
		}
		switch req.action {
		case actionQuit:
			quit = true
		case actionUpdateUserInfo:
			updateReq, ok := req.req.(UpdateUserInfoReq)
			if !ok {
				panic("update req should be UpdateUserInfoReq")
			}
			sys := updateReq.sys
			cb, err := sys.ObtainChan(updateReq.user)
			if err != nil && errors.Is(err, errNotNeedUpdate){
				panic(err)
			}
			sys.Lock()
			if err := sys.loadUserWithoutLock(updateReq.user); err != nil {
				uinfo, ok := sys.users[updateReq.user]
				if !ok {
					panic(fmt.Errorf("info should in cache"))
				}
				if time.Since(uinfo.StartUpdate) > sys.stay * tenTimes {
					sys.dropUserWithoutLock(updateReq.user)
				}
				cb <- err
			}
			cb <- nil
			sys.Unlock()
		default:
			panic(fmt.Errorf("action type not support %v", req.action))
		}
	}
}

func (sys *IAMSys) dropUserWithoutLock(user string) {
	uinfo, ok := sys.users[user]
	if !ok {
		panic(fmt.Errorf("info should in cache"))
	}
	uinfo.Valid = false
	uinfo.StartUpdate = time.Unix(0,0)
}

func startUpdateUserInfo(sys *IAMSys, user string, c chan error) {
	sys.Lock()
	defer sys.Unlock()
	if err := sys.loadUserWithoutLock(user); err != nil {
		uinfo, ok := sys.users[user]
		if !ok {
			panic("info should in cache")
		}
		if uinfo.StartUpdate.Equal(time.Unix(0, 0)) {
			panic("update should start")
		}
		if time.Since(uinfo.StartUpdate) > sys.stay * tenTimes {
			sys.dropUserWithoutLock(user)
		}
		c <- err
	}
	c <- nil
}

func (sys *IAMSys) doLoadUser(user string) (wait bool, c chan error, err error) {
	doUpdate := false
	sys.Lock()
	defer sys.Lock()
	uinfo, ok := sys.users[user]
	if !ok {
		//should first and block for this request
		sys.users[user] = &CacheUserInfo {
			Valid: false,
			StayInCache: time.Unix(0, 0),
			StartUpdate: time.Unix(0, 0),
		}
		wait = true
		doUpdate = true
		uinfo = sys.users[user]
	}

	if !uinfo.Valid {
		wait = true
		doUpdate = true
	}

	if time.Since(uinfo.StayInCache) > sys.stay {
		doUpdate = true
		if uinfo.StartUpdate.Equal(time.Unix(0,0)) {
			wait = true
		}
	}

	if doUpdate {
		c, ok = sys.waiters[user]
		if !ok {
			c = make(chan error)
			sys.waiters[user] = c
		}
		if uinfo.StartUpdate.Equal(time.Unix(0,0)) {
			uinfo.StartUpdate = time.Now()
			if sys.background {
				ureq := UpdateUserInfoReq {
					user: user,
					sys: sys,
				}
				wreq := WorkRequest {
					action: actionUpdateUserInfo,
					req:ureq,
				}
				sys.workqueue <- wreq
			} else {
				go startUpdateUserInfo(sys, user, c)
			}
		}
	}

	return wait, c, nil
}

func(sys *IAMSys) getNegative() time.Duration {
	sys.RLock()
	defer sys.RUnlock()
	res := sys.negative
	return res
}


func (sys *IAMSys) waitForUpdate(user string, c chan error) {
	for {
		res, ok := <-c
		//close chan
		if !ok {
			break
		}
		if res == nil {
			sys.Lock()
			defer sys.Unlock()
			delete(sys.waiters, user)
			close(c)
			break
		}
		log.Printf("get failed err %v", res)
		t := sys.getNegative()
		time.Sleep(t)
		sys.Lock()
		defer sys.Unlock()
		uinfo, ok := sys.users[user]
		if !ok || uinfo.StartUpdate.Equal(time.Unix(0, 0)) {
			panic("update should start")
		}
		if uinfo.StartUpdate.Equal(time.Unix(0,0)){
			uinfo.StartUpdate = time.Now()
		}
		if sys.background {
			ureq := UpdateUserInfoReq {
				user: user,
				sys: sys,
			}
			wreq := WorkRequest {
				action: actionUpdateUserInfo,
				req:ureq,
			}
			sys.workqueue <- wreq
		} else {
			go startUpdateUserInfo(sys, user, c)
		}
	}
}


func (sys *IAMSys) GetGroupsByUser(user string) ([]string, error) {
	sys.RLock()
	uinfo, ok := sys.users[user]
	if !ok || !uinfo.Valid || time.Since(uinfo.StayInCache) > sys.stay {
		sys.RUnlock()
		// should to update info
		// may be all need to wait
		wait, c, err := sys.doLoadUser(user)
		if err != nil {
			return []string{}, err
		}
		if wait {
			sys.waitForUpdate(user, c)
		}
	}
	sys.RLock()
	defer sys.RUnlock()
	uinfo, ok = sys.users[user]
	if !ok || !uinfo.Valid {
		panic(fmt.Errorf("should be in cache"))
	}
	res := make([]string, len(uinfo.Groups))
	copy(res, uinfo.Groups)
	return res, nil

	return uinfo.Groups, nil
}

// there is should have write lock without this func
func (sys *IAMSys) updateUserInfo(user string, info *opfsUserInfo) error {
	uinfo, ok := sys.users[user]
	if !ok {
		cinfo := &CacheUserInfo {
			Valid: true,
			StayInCache: time.Now(),
			StartUpdate: time.Unix(0, 0),
			Uid: info.uid,
			Groups: make([]string, len(info.groups)),
		}
		copy(cinfo.Groups, info.groups)
		sys.users[user] = cinfo
		return nil
	}
	uinfo.Valid = true
	uinfo.StayInCache = time.Now()
	uinfo.StartUpdate = time.Unix(0,0)
	uinfo.Uid = info.uid
	uinfo.Groups = make([]string, len(info.groups))
	copy(uinfo.Groups, info.groups)

	return nil
}

func (sys *IAMSys) LoadUsers() error {
	sys.Lock()
	defer sys.Unlock()
	ctx := context.Background()
	userinfos, err := opfsLoadUsers(ctx)
	if err != nil {
		return err
	}
	for _, uinfo := range userinfos {
		if err := sys.updateUserInfo(uinfo.uname, uinfo); err != nil {
			return err
		}
	}

	return err
}

func (sys *IAMSys) checkValidGroups(groups []string) error {
	ctx := context.Background()
	validGroups, err := opfsLoadGroups(ctx)
	if err != nil {
		return err
	}
	for _, g := range groups {
		found := false
		for _, vg := range validGroups {
			if g == vg {
				found = true
				break
			}
		}
		if !found {
			log.Printf("invalid group g %v validGroups %v", g, validGroups)
			return os.ErrNotExist
		}
	}

	return nil
}

func (sys *IAMSys) makeRootGroups(groups []string) error {
	nmap := make(map[string]struct{})
	for _, g := range groups {
		nmap[g] = struct{}{}
	}
	sys.Lock()
	defer sys.Unlock()
	sys.rootgroups = nmap

	return nil
}

func (sys *IAMSys) ReloadRootGroups(groups []string) error {
	if err := sys.checkValidGroups(groups); err != nil {
		return err
	}
	if err := sys.makeRootGroups(groups); err != nil {
		return err
	}
	return nil
}


func (sys *IAMSys) Init() error {
	if err := sys.LoadUsers(); err != nil {
		return err
	}
	if sys.background {
		for i := 0; i < sys.backthread; i++ {
			go threadWork(sys.workqueue)
		}
	}

	return nil
}

func (sys *IAMSys) printDebugInfo() {

	log.Printf("sys head %#v", *sys)
	log.Printf("userMap:")
	for k, v := range sys.users {
		log.Printf("k %v, v %#v", k, *v)
	}
	log.Printf("waiters:")
	for k, v := range sys.waiters {
		log.Printf("k %v, v %v", k, v)
	}
	log.Printf("superuser groups:")
	for k, _ := range sys.rootgroups {
		log.Printf("k %v", k)
	}
}

func NewIAMSys(conf *IAMSysConf) *IAMSys {
	sys := &IAMSys {
		RWMutex: &sync.RWMutex{},
		stay: conf.Stay,
		background: conf.BackGround,
		backthread: conf.BackGroundThread,
		negative: conf.Negative,
		workqueue: make(chan WorkRequest),
		users: make(map[string]*CacheUserInfo),
		waiters: make(map[string]chan error),
	}

	if err := sys.Init(); err != nil {
		return nil
	}

	if err := sys.ReloadRootGroups(conf.RootGroups); err != nil {
		return nil
	}

	sys.printDebugInfo()

	return sys
}
