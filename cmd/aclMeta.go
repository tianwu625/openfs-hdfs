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

type opfsHdfsNamespaceQuota struct {
	SetQuota bool `json:"setQuota, omitempty"`
	Quota uint64 `jsong:"numQuota, omitempty"`
}

type opfsHdfsMeta struct {
	Acl *opfsHdfsAcl `json:"acl, omitempty"`
	Quota *opfsHdfsNamespaceQuota `json:"quota, omitempty"`
}

type opfsMetaCacheEntry struct {
	UpdateTime time.Time //file modify time
	ConfigTime time.Time //config file modify time
	StayTime time.Time
	meta *opfsHdfsMeta
}

const (
	DefaultMaxEntries = 1000
	DefaultLifeTime = 60
)

type opfsMetaCache struct {
	MaxEntries int
	CurrentEntries int
	*sync.RWMutex
	metas map[string]*opfsMetaCacheEntry
}

var (
	errNotPurge error = errors.New("Full but not purge any entry")
	errNoSpace error = errors.New("No space for new entry in cache")
	errCacheInvalid error = errors.New("cache entry is invalid")
)
//this func need write lock
//puragecount == 0 or negative, clean all invalid entries
func (c *opfsMetaCache) purgeEntryWithoutLock(purgecount int) error {
	current := 0
	for k, v := range c.metas {
		if c.ValidCacheEntry(k, v) {
			continue
		}
		delete(c.metas, k)
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

func (c *opfsMetaCache) haveCacheEntry() bool {
	if c.CurrentEntries == c.MaxEntries {
		err := c.purgeEntryWithoutLock(1)
		if err != nil && !errors.Is(err, errNotPurge){
			log.Printf("drop fail %v", err)
			return  false
		}
	}

	return true
}

func (c *opfsMetaCache) loadCacheEntry (src string) (*opfsMetaCacheEntry, error) {
	e, err := opfsGetMetaEntry(src)
	if err != nil {
		return nil, err
	}
	olde, ok := c.metas[src]
	if ok {
		if c.ValidCacheEntry(src, olde) {
			log.Printf("another thread to load this entry, this thread do nothing")
			log.Printf("return entry %v in cache", olde)
			return olde, nil
		}
		c.metas[src] = e
	} else if c.haveCacheEntry() {
		c.metas[src] = e
		c.CurrentEntries++
	} else {
		err = errNoSpace
	}

	return e, err
}

func (c *opfsMetaCache) getValidCacheEntry(src string) (*opfsMetaCacheEntry, error) {
	e, ok := c.metas[src]
	if ok && c.ValidCacheEntry(src, e) {
		return e, nil
	}

	if !ok {
		return nil, errNotFound
	}
	//in cache, but invalid
	return nil, errCacheInvalid
}

func (c *opfsMetaCache) validModifyTime(src string, e *opfsMetaCacheEntry) bool {
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

func (c *opfsMetaCache) validStayTime(e *opfsMetaCacheEntry) bool {
	elapse := time.Since(e.StayTime)
	log.Printf("staytime %v elapse %v", e.StayTime, elapse)
	if elapse >= DefaultLifeTime * time.Second {
		return false
	}

	return true
}

func (c *opfsMetaCache) ValidCacheEntry(src string, e *opfsMetaCacheEntry) bool {
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

func (c *opfsMetaCache) UpdateStayTime(src string) {
	c.Lock()
	defer c.Unlock()
	e, err := c.getValidCacheEntry(src)
	if err != nil {
		return
	}
	log.Printf("update before staytime %v", e.StayTime)
	e.StayTime = time.Now()
	log.Printf("update staytime %v", e.StayTime)
}

func (c *opfsMetaCache) GetAcl(src string) (opfsHdfsAcl, error) {
	c.RLock()
	defer c.RUnlock()
	e, err := c.getCacheEntry(src, false)
	if err == nil {
		//valid entry
		go c.UpdateStayTime(src)
		return *e.meta.Acl, nil
	}
	e, err = opfsGetMetaEntry(src)
	if err != nil {
		return opfsHdfsAcl{}, err
	}

	go c.getCacheEntry(src, true)

	return *e.meta.Acl, nil
}

//include 2 part: 1. get valid entry 2. load a entry from storage
//invoker with write lock can load
//invoker with read lock can't load
func (c *opfsMetaCache) getCacheEntry(src string, withload bool) (*opfsMetaCacheEntry, error) {
	e, err := c.getValidCacheEntry(src)
	if err == nil {
		return e, nil
	}

	log.Printf("get valid fail %v", err)

	if withload {
		return c.loadCacheEntry(src)
	}

	return e, err
}

func (c *opfsMetaCache) setAcl(src string, acl *opfsHdfsAcl) error {
	c.Lock()
	defer c.Unlock()

	e, err := c.getCacheEntry(src, true)
	if err != nil {
		if !errors.Is(err, errNoSpace) {
			return err
		}
	}
	e.meta.Acl = acl

	if err := opfsStoreConfig(src, e.meta); err != nil {
		return err
	}

	e.StayTime = time.Now()
	e.UpdateTime, err = opfsGetModifyTime(src)
	if err != nil {
		return err
	}

	return nil
}

func (c *opfsMetaCache) SetAcl(src string, acl opfsHdfsAcl) error {
	return c.setAcl(src, &acl)
}

func (c *opfsMetaCache) setNamespaceQuota(src string, quota *opfsHdfsNamespaceQuota) error {
	c.Lock()
	defer c.Unlock()

	e, err := c.getCacheEntry(src, true)
	if err != nil {
		if !errors.Is(err, errNoSpace) {
			return err
		}
	}
	e.meta.Quota = quota

	if err := opfsStoreConfig(src, e.meta); err != nil {
		return err
	}

	e.StayTime = time.Now()
	e.UpdateTime, err = opfsGetModifyTime(src)
	if err != nil {
		return err
	}

	return nil
}

func (c *opfsMetaCache) SetNamespaceQuota(src string, quota opfsHdfsNamespaceQuota) error {
	return c.setNamespaceQuota(src, &quota)
}

func (c *opfsMetaCache) GetNamespaceQuota(src string) (opfsHdfsNamespaceQuota, error) {
	c.RLock()
	defer c.RUnlock()
	e, err := c.getCacheEntry(src, false)
	if err != nil {
		go c.UpdateStayTime(src)
		return *e.meta.Quota, nil
	}
	e, err = opfsGetMetaEntry(src)
	if err != nil {
		return opfsHdfsNamespaceQuota{}, err
	}

	go c.getCacheEntry(src, true)

	return *e.meta.Quota, nil
}

func (c *opfsMetaCache) Delete (src string) error {
	c.Lock()
	defer c.Unlock()
	delete(c.metas, src)
	return opfsDeleteMetaEntry(src)
}

func (c *opfsMetaCache) Rename(src, dst string) error {
	c.Lock()
	defer c.Unlock()
	if err := opfsRenameMetaPath(src, dst); err != nil {
		return err
	}
	e, ok := c.metas[src]
	if ok {
		c.metas[dst] = e
	}
	delete(c.metas, src)

	return nil
}

func initMetaCache() *opfsMetaCache {
	return &opfsMetaCache {
		MaxEntries: DefaultMaxEntries,
		CurrentEntries: 0,
		RWMutex: &sync.RWMutex{},
		metas: make(map[string]*opfsMetaCacheEntry),
	}
}
