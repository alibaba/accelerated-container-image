/*
   Copyright The Accelerated Container Image Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

package p2p

import (
	"container/list"
	"io"
	"os"
	"path"
	"sync"

	log "github.com/sirupsen/logrus"
)

type syncMap interface {
	Get(key string) (interface{}, bool)
	Set(key string, val interface{})
	GetOrSet(key string, creator func(key string) (interface{}, error)) (interface{}, error)
	Remove(key string)
}

type rwSyncMapItem struct {
	val interface{}
	mtx sync.RWMutex
}

type rwSyncMap struct {
	kv   map[string]*rwSyncMapItem
	lock sync.RWMutex
}

func (m *rwSyncMap) Get(key string) (interface{}, bool) {
	m.lock.RLock()
	val, hit := m.kv[key]
	m.lock.RUnlock()
	if !hit {
		return nil, false
	}
	val.mtx.RLock()
	defer val.mtx.RUnlock()
	if val.val == nil {
		return nil, false
	}
	return val.val, true
}

func (m *rwSyncMap) Set(key string, val interface{}) {
	if val == nil {
		return
	}
	m.lock.Lock()
	defer m.lock.Unlock()
	m.kv[key] = &rwSyncMapItem{val: val}
}

func (m *rwSyncMap) Remove(key string) {
	m.lock.Lock()
	defer m.lock.Unlock()
	delete(m.kv, key)
}

func (m *rwSyncMap) GetOrSet(key string, creator func(key string) (interface{}, error)) (interface{}, error) {
	ret, hit := m.Get(key)
	if hit {
		if ret == nil {
			log.Fatal("Store nil")
		}
		return ret, nil
	}
	m.lock.Lock()
	item, hit := m.kv[key]
	if !hit {
		item = &rwSyncMapItem{val: nil}
		m.kv[key] = item
	}
	m.lock.Unlock()
	item.mtx.Lock()
	defer item.mtx.Unlock()
	var err error
	if item.val == nil {
		ret, err = creator(key)
		if err != nil {
			return nil, err
		}
		item.val = ret
	}
	return item.val, err
}

type cacheItem interface {
	Key() string
	Val() interface{}
	Size() int64
	Drop()
}

type lruCache interface {
	Get(key string) (cacheItem, bool)
	Set(key string, val cacheItem)
	GetOrSet(key string, creator func(key string) (cacheItem, error)) (cacheItem, error)
	Del(key string)
	Touch(key string)
	Sum() int64
	Limit() int64
	Expire()
}

type lruSyncMapCache struct {
	kv       syncMap // map of `*list.Element` stores `CacheItem`
	l        *list.List
	lock     sync.Mutex
	s, limit int64
}

func newLRUCache(limit int64) lruCache {
	return &lruSyncMapCache{
		kv:    &rwSyncMap{kv: make(map[string]*rwSyncMapItem)},
		l:     list.New(),
		limit: limit,
	}
}

func (m *lruSyncMapCache) Del(key string) {
	elem, hit := m.kv.Get(key)
	if hit {
		m.lock.Lock()
		defer m.lock.Unlock()
		v := m.l.Remove(elem.(*list.Element))
		item := v.(cacheItem)
		m.s -= item.Size()
		m.kv.Remove(key)
		item.Drop()
	}
}

func (m *lruSyncMapCache) Get(key string) (cacheItem, bool) {
	defer m.Expire()
	elem, hit := m.kv.Get(key)
	if hit {
		m.lock.Lock()
		defer m.lock.Unlock()
		m.l.MoveToFront(elem.(*list.Element))
		return elem.(*list.Element).Value.(cacheItem), true
	}
	return nil, false
}

func (m *lruSyncMapCache) Set(key string, val cacheItem) {
	defer m.Expire()
	if val == nil {
		return
	}
	elem, err := m.kv.GetOrSet(key, func(_ string) (interface{}, error) {
		m.lock.Lock()
		defer m.lock.Unlock()
		m.s += val.Size()
		return m.l.PushFront(val), nil
	})
	if err != nil {
		return
	}
	e := elem.(*list.Element)
	m.lock.Lock()
	defer m.lock.Unlock()
	if e.Value != nil {
		m.s -= e.Value.(cacheItem).Size()
	}
	e.Value = val
	m.s += val.Size()
	m.l.MoveToFront(e)
}

func (m *lruSyncMapCache) GetOrSet(key string, creator func(key string) (cacheItem, error)) (cacheItem, error) {
	defer m.Expire()
	elem, err := m.kv.GetOrSet(key, func(_ string) (interface{}, error) {
		val, err := creator(key)
		if err != nil {
			return nil, err
		}
		m.lock.Lock()
		defer m.lock.Unlock()
		m.s += val.Size()
		ret := m.l.PushFront(val)
		return ret, err
	})
	if err != nil {
		return nil, err
	}
	if elem == nil {
		log.Fatal("ERROR, err", err)
	}
	e := elem.(*list.Element)
	m.lock.Lock()
	defer m.lock.Unlock()
	m.l.MoveToFront(e)
	return e.Value.(cacheItem), err
}

func (m *lruSyncMapCache) Touch(key string) {
	m.Get(key)
}

func (m *lruSyncMapCache) Sum() int64 {
	return m.s
}

func (m *lruSyncMapCache) Limit() int64 {
	return m.limit
}

func (m *lruSyncMapCache) Expire() {
	m.lock.Lock()
	defer m.lock.Unlock()
	if m.s > m.limit {
		for m.s > m.limit {
			front := m.l.Remove(m.l.Front())
			if front == nil {
				return
			}
			item := front.(cacheItem)
			m.s -= item.Size()
			m.kv.Remove(item.Key())
			item.Drop()
		}
	}
}

type fileCacheItem struct {
	key      string
	fileSize int64
	file     *os.File
	lock     sync.RWMutex
}

func (f *fileCacheItem) Drop() {
	f.lock.Lock()
	defer f.lock.Unlock()
	if err := os.Remove(f.file.Name()); err != nil {
		log.Warnf("File %s remove error!", f.file.Name())
	}
}

func (f *fileCacheItem) Size() int64 {
	return f.fileSize
}

func (f *fileCacheItem) Key() string {
	return f.key
}

func readAllByHead(file *os.File) ([]byte, error) {
	offset := int64(0)
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	ret := make([]byte, info.Size())
	for offset < info.Size() && err == nil {
		var l int
		l, err = file.ReadAt(ret[offset:], offset)
		offset += int64(l)
	}
	if err == io.EOF {
		err = nil
	}
	return ret[:offset], err
}

func (f *fileCacheItem) Val() interface{} {
	f.lock.RLock()
	defer f.lock.RUnlock()
	ret, err := readAllByHead(f.file)
	if err != nil {
		return nil
	}
	return ret
}

func writeAll(file *os.File, buff []byte) (int, error) {
	offset := 0
	err := file.Truncate(0)
	if err != nil {
		return 0, err
	}
	for err == nil && offset < len(buff) {
		l := 0
		l, err = file.Write(buff[offset:])
		offset += l
	}
	return offset, err
}

func newFileCacheItem(key string, fileSize int64, read func() ([]byte, error)) (*fileCacheItem, error) {
	err := os.MkdirAll(path.Dir(key), 0755)
	if err != nil {
		return nil, err
	}
	file, err := os.OpenFile(key, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil || file == nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() != fileSize {
		buffer, err := read()
		if err == nil && int64(len(buffer)) != fileSize {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			if err := os.Remove(file.Name()); err != nil {
				log.Warnf("File %s remove error!", file.Name())
			}
			return nil, err
		}
		l, err := writeAll(file, buffer)
		if err == nil && int64(l) != fileSize {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			if err := os.Remove(file.Name()); err != nil {
				log.Warnf("File %s remove error!", file.Name())
			}
			return nil, err
		}
	}
	item := &fileCacheItem{key: key, fileSize: fileSize, file: file}
	return item, err
}

type fileCacheLRU struct {
	cache lruCache
}

func newFileCacheLRU(limit int64) *fileCacheLRU {
	return &fileCacheLRU{newLRUCache(limit)}
}

func (c *fileCacheLRU) Get(key string) (*fileCacheItem, bool) {
	ret, hit := c.cache.Get(key)
	return ret.(*fileCacheItem), hit
}

func (c *fileCacheLRU) Set(key string, val *fileCacheItem) {
	c.cache.Set(key, val)
}

func (c *fileCacheLRU) GetOrSet(key string, creator func(key string) (*fileCacheItem, error)) (*fileCacheItem, error) {
	ret, err := c.cache.GetOrSet(key, func(key string) (cacheItem, error) { return creator(key) })
	if err != nil {
		return nil, err
	}
	return ret.(*fileCacheItem), err
}

func (c *fileCacheLRU) Touch(key string) {
	c.cache.Touch(key)
}

func (c *fileCacheLRU) Sum() int64 {
	return c.cache.Sum()
}

func (c *fileCacheLRU) Limit() int64 {
	return c.cache.Limit()
}

func (c *fileCacheLRU) Expire() {
	c.cache.Expire()
}

func (c *fileCacheLRU) Del(key string) {
	c.cache.Del(key)
}

type memCacheItem struct {
	key string
	val interface{}
}

func (f *memCacheItem) Drop() {
}

func (f *memCacheItem) Size() int64 {
	return 1
}

func (f *memCacheItem) Key() string {
	return f.key
}

func (f *memCacheItem) Val() interface{} {
	return f.val
}

func newMemCacheItem(key string, val interface{}) *memCacheItem {
	return &memCacheItem{key, val}
}

type memCacheLRU struct {
	cache lruCache
}

func newMemCacheLRU(limit int64) *memCacheLRU {
	return &memCacheLRU{newLRUCache(limit)}
}

func (c *memCacheLRU) Get(key string) (*memCacheItem, bool) {
	ret, hit := c.cache.Get(key)
	if ret == nil {
		return nil, hit
	}
	return ret.(*memCacheItem), true
}

func (c *memCacheLRU) Set(key string, val *memCacheItem) {
	c.cache.Set(key, val)
}

func (c *memCacheLRU) GetOrSet(key string, creator func(key string) (*memCacheItem, error)) (*memCacheItem, error) {
	ret, err := c.cache.GetOrSet(key, func(key string) (cacheItem, error) { return creator(key) })
	if ret == nil {
		return nil, err
	}
	return ret.(*memCacheItem), err
}

func (c *memCacheLRU) Touch(key string) {
	c.cache.Touch(key)
}

func (c *memCacheLRU) Sum() int64 {
	return c.cache.Sum()
}

func (c *memCacheLRU) Limit() int64 {
	return c.cache.Limit()
}

func (c *memCacheLRU) Expire() {
	c.cache.Expire()
}

func (c *memCacheLRU) Del(key string) {
	c.cache.Del(key)
}
