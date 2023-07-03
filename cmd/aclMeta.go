package cmd

import (
	"time"
	"sync"
	"errors"
	"log"
)

type opfsHdfsAclEntry struct {
	AclType string `json:"type"`
	AclScope string `json:"scope"`
	AclPerm uint32 `json:"perm`
	AclName string `json:"name"`
}

type opfsHdfsAcl struct {
	SetMask bool `json:"setmask, omitempty"`
	Mask uint32 `json:"mask,omitempty"`
	SetGroup bool `json:"setgroup, omitempty"`
	GroupPerm uint32 `jons:"groupperm, omitempty"`
	Entries []opfsHdfsAclEntry `json:"entrys,omitempty"`
	SetDefaultMask bool `json:"setdmask, omitempty"`
	DefaultMask uint32 `json:"dmask, omitempty"`
	DefaultEntries []opfsHdfsAclEntry `json:"dentrys, omitempty"`
	SetDefaultUser bool `json:"setdefaultuser, omitempty"`
	DefaultUser uint32 `json:"defaultuser, omitempty"`
	SetDefaultGroup bool `json:"setdefaultgroup, omitempty"`
	DefaultGroup uint32 `json:"defaultgroup, omitempty"`
	SetDefaultOther bool `json:"setdefaultother, omitempty"`
	DefaultOther uint32 `json:"defaultother, omitempty"`
}

type opfsAclCacheEntry struct {
	UpdateTime time.Time //file modify time
	ConfigTime time.Time //config file modify time
	StayTime time.Time
	Acl *opfsHdfsAcl
}

const (
	DefaultMaxEntries = 1000
	DefaultLifeTime = 60
)

type opfsAclCache struct {
	MaxEntries int
	CurrentEntries int
	*sync.RWMutex
	Acls map[string]*opfsAclCacheEntry
}

var (
	errNotPurge error = errors.New("Full but not purge any entry")
	errInvalidParameter error = errors.New("Invalid parameter")
)

func (c *opfsAclCache) purgeEntryWithoutLock(purgecount int) error {
	if purgecount == 0 {
		return errInvalidParameter
	}
	current := 0
	for k, v := range c.Acls {
		if c.ValidCacheEntry(k, v) {
			continue
		}
		delete(c.Acls, k)
		c.CurrentEntries--
		current++
		if current == purgecount && purgecount > 0 {
			break
		}
	}
	if current == 0 {
		return errNotPurge
	}

	return nil
}

func (c *opfsAclCache) haveCacheEntry() bool {
	if c.CurrentEntries == c.MaxEntries {
		err := c.purgeEntryWithoutLock(1)
		if err != nil && !errors.Is(err, errNotPurge){
			log.Printf("drop fail %v", err)
			return  false
		}
	}

	return true
}

func (c *opfsAclCache) LoadAclCacheEntry(src string) (opfsHdfsAcl, error){
	c.Lock()
	defer c.Unlock()

	if e, ok := c.Acls[src]; ok {
		return *e.Acl, nil
	}
	e, err := opfsGetAclEntry(src)
	if err != nil {
		return opfsHdfsAcl{}, err
	}
	if c.haveCacheEntry() {
		c.Acls[src] = e
		c.CurrentEntries++
	}

	return *e.Acl, nil
}

func (c *opfsAclCache) validModifyTime(src string, e *opfsAclCacheEntry) bool {
	m, err := opfsGetModifyTime(src)
	if err != nil {
		log.Printf("get modify %v err %v", src, err)
		return false
	}
	if e.UpdateTime.Before(m) {
		log.Printf("update %v %v", e.UpdateTime.Unix(), m.Unix())
		return false
	}
	srcMeta := opfsGetMetaPath(src)
	m, err = opfsGetModifyTime(srcMeta)
	if err != nil {
		log.Printf("srcMeta %v err %v", srcMeta, err)
		return false
	}
	if e.ConfigTime.Before(m) {
		log.Printf("config %v %v", e.ConfigTime.Unix(), m.Unix())
		return false
	}
	return true
}

func (c *opfsAclCache) validStayTime(e *opfsAclCacheEntry) bool {
	elapse := time.Since(e.StayTime)
	log.Printf("staytime %v elapse %v", e.StayTime, elapse)
	if elapse >= DefaultLifeTime * time.Second {
		return false
	}

	return true
}

func (c *opfsAclCache) ValidCacheEntry(src string, e *opfsAclCacheEntry) bool {
	if !c.validModifyTime(src, e) {
		log.Printf("modify time invalid")
		return false
	}

	if !c.validStayTime(e) {
		log.Printf("stay time invalid")
		return false
	}

	return true
}

func (c *opfsAclCache) getAclCacheEntry(src string) (*opfsHdfsAcl, error) {
	c.RLock()
	defer c.RUnlock()

	e, ok := c.Acls[src]
	if ok {
		if !c.ValidCacheEntry(src, e) {
			log.Printf("valid check fail %v %v", src, e)
			return nil, errNotFound
		}
		return e.Acl, nil
	}

	return nil, errNotFound
}

func (c *opfsAclCache) UpdateStayTime(src string) {
	c.Lock()
	defer c.Unlock()
	e, ok := c.Acls[src]
	if !ok {
		return
	}
	log.Printf("update before staytime %v", e.StayTime)
	e.StayTime = time.Now()
	log.Printf("update staytime %v", e.StayTime)
}

func (c *opfsAclCache) GetAclCacheEntry(src string) (opfsHdfsAcl, error) {
	acl, err := c.getAclCacheEntry(src)
	if err != nil {
		log.Printf("in cache %v", err)
		if errors.Is(err, errNotFound) {
			return c.LoadAclCacheEntry(src)
		}
		return opfsHdfsAcl{}, err
	}

	go c.UpdateStayTime(src)

	return *acl, nil
}

func (c *opfsAclCache) setEntryWithoutLock(src string, acl *opfsHdfsAcl) error {
	update, err := opfsGetModifyTime(src)
	if err != nil {
		return err
	}
	e := &opfsAclCacheEntry {
		UpdateTime: update,
		StayTime: time.Now(),
		Acl: acl,
	}
	if c.haveCacheEntry() {
		log.Printf("set to entry src %v e %v", src, e)
		c.Acls[src] = e
		c.CurrentEntries++
	}

	return nil

}

func (c *opfsAclCache) updateEntryWithoutLock(src string, e *opfsAclCacheEntry, acl *opfsHdfsAcl) error {
	var err error
	e.UpdateTime = time.Now()
	e.Acl = acl
	e.UpdateTime, err = opfsGetModifyTime(src)
	if err != nil {
		return err
	}
	c.Acls[src] = e

	return nil
}

func (c *opfsAclCache) setAclCacheEntry(src string, acl *opfsHdfsAcl) error {
	if err := opfsStoreConfig(src, acl); err != nil {
		return err
	}
	c.Lock()
	defer c.Unlock()
	e, ok := c.Acls[src]
	if !ok {
		return c.setEntryWithoutLock(src, acl)
	}
	return c.updateEntryWithoutLock(src, e, acl)
}

func (c *opfsAclCache) SetAclCacheEntry(src string, acl opfsHdfsAcl) error {
	return c.setAclCacheEntry(src, &acl)
}

func InitAclCache() *opfsAclCache {
	return &opfsAclCache {
		MaxEntries: DefaultMaxEntries,
		CurrentEntries: 0,
		RWMutex: &sync.RWMutex{},
		Acls: make(map[string]*opfsAclCacheEntry),
	}
}
