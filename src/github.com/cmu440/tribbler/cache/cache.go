package cache

import (
	"container/list"
	"errors"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"sync"
	"time"
)

type Cache struct {
	mu      sync.Mutex
	datamap map[string]*Entry
}

type Entry struct {
	granted bool
	val     interface{}
	query   *list.List
	lock    sync.Mutex

	leaseTime time.Time
	leaseDur  time.Duration
}

func NewCache() *Cache {
	cache := new(Cache)
	cache.datamap = make(map[string]*Entry)

	return cache
}

func (cache *Cache) Get(key string, args *storagerpc.GetArgs) (interface{}, error) {
	///cache.mu.Lock()

	// it first clear the expired keys
	cache.Clear()

	// check if there exist key
	entry, ok := cache.datamap[key]

	if ok == false {
		///cache.mu.Unlock()

		entry = new(Entry)
		entry.query = list.New()
		entry.query.PushBack(time.Now())

		///cache.mu.Lock()
		cache.datamap[key] = entry

		///cache.mu.Unlock()
		return "", errors.New("KeyNotFound")
	}

	///cache.mu.Unlock()
	// check if lease is granted

	///entry.lock.Lock()
	if entry.granted == true {
		val := entry.val
		//cache.mu.Unlock()
		///entry.lock.Unlock()
		return val, nil
	}

	entry.query.PushBack(time.Now())

	if entry.query.Len() > storagerpc.QueryCacheThresh {
		args.WantLease = true
	}

	//cache.mu.Unlock()
	///entry.lock.Unlock()
	return "", errors.New("KeyNotFound")
}

func (cache *Cache) Insert(key string, val interface{}, lease storagerpc.Lease) {
	///cache.mu.Lock()

	// first check if there exist key
	entry, ok := cache.datamap[key]
	if ok == false {
		entry = new(Entry)
		entry.query = list.New()

		cache.datamap[key] = entry
	}

	///cache.mu.Unlock()

	// insert values
	///entry.lock.Lock()

	entry.granted = true
	entry.val = val
	entry.leaseTime = time.Now()
	entry.leaseDur = time.Duration(lease.ValidSeconds) * time.Second

	///entry.lock.Unlock()

	//cache.mu.Unlock()
}

func (cache *Cache) Clear() {
	for key, entry := range cache.datamap {
		if entry.granted == true {
			dur := time.Since(entry.leaseTime)
			if dur > entry.leaseDur {
				entry.granted = false
			}
		}

		elem := entry.query.Front()
		for elem != nil {
			tempdur := time.Since(elem.Value.(time.Time))
			if tempdur > time.Duration(storagerpc.QueryCacheSeconds)*time.Second {
				_ = entry.query.Remove(elem)
				elem = entry.query.Front()
			} else {
				break
			}
		}

		if entry.query.Len() == 0 {
			delete(cache.datamap, key)
		}
	}
}

func (cache *Cache) Revoke(key string) bool {
	///cache.mu.Lock()

	entry, ok := cache.datamap[key]
	///cache.mu.Unlock()

	//entry.lock.Lock()
	if ok == true {
		///entry.lock.Lock()
		entry.granted = false
		///entry.lock.Unlock()
	}

	//cache.mu.Unlock()
	return ok
}
