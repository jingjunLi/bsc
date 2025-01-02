package snapshot

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
)

// slicePool is a shared pool of hash slice, for reducing the GC pressure.
var slicePool = sync.Pool{
	New: func() interface{} {
		slice := make([]*diffLayer, 0, 16) // Pre-allocate a slice with a reasonable capacity.
		return &slice
	},
}

// getSlice obtains the hash slice from the shared pool.
func getSlice() []*diffLayer {
	slice := *slicePool.Get().(*[]*diffLayer)
	slice = slice[:0]
	return slice
}

// returnSlice returns the hash slice back to the shared pool for following usage.
func returnSlice(slice []*diffLayer) {
	// Discard the large slice for recycling
	if len(slice) > 128 {
		return
	}
	// Reset the slice before putting it back into the pool
	slicePool.Put(&slice)
}

// slicePool is a shared pool of hash slice, for reducing the GC pressure.
var sliceMapPool = sync.Pool{
	New: func() interface{} {
		slice := make(map[common.Hash][]*diffLayer, 16) // Pre-allocate a slice with a reasonable capacity.
		return &slice
	},
}

//// getSlice obtains the hash slice from the shared pool.
//func getSliceMap() map[common.Hash][]*diffLayer {
//	slice := *sliceMapPool.Get().(*map[common.Hash][]*diffLayer)
//	return slice
//}
//
//func returnMapToPool(m map[common.Hash][]*diffLayer) {
//	// 清空 map
//	for k := range m {
//		delete(m, k)
//	}
//	sliceMapPool.Put(m)
//}

const shardCount = 32

type ShardLock struct {
	locks [shardCount]sync.Mutex
}

func (s *ShardLock) getLock(hash common.Hash) *sync.Mutex {
	return &s.locks[accountBloomHash(hash)%shardCount]
}

// diffAncestors returns all the ancestors of the specific layer (disk layer
// is not included).
func collectDiffLayerAncestors(layer Snapshot) map[common.Hash]struct{} {
	set := make(map[common.Hash]struct{})
	for {
		parent := layer.Parent()
		if parent == nil {
			break // finished
		}
		if _, ok := parent.(*diskLayer); ok {
			break // finished
		}
		set[parent.Root()] = struct{}{}
		layer = parent
	}
	return set
}

// Lookup is an internal help structure to quickly identify
type Lookup struct {
	stateToLayerAccount map[common.Hash][]*diffLayer                 // 2w
	stateToLayerStorage map[common.Hash]map[common.Hash][]*diffLayer // 5k
	descendants         map[common.Hash]map[common.Hash]struct{}

	layers map[common.Hash]struct{}

	lock sync.RWMutex
	//descendantsLock ShardLock
}

// newLookup initializes the lookup structure.
func newLookup(head Snapshot) *Lookup {
	l := new(Lookup)

	{ // setup state mapping
		var (
			current = head
			layers  []Snapshot
		)
		for current != nil {
			layers = append(layers, current)
			current = current.Parent()
		}
		l.stateToLayerAccount = make(map[common.Hash][]*diffLayer, 60000)
		l.stateToLayerStorage = make(map[common.Hash]map[common.Hash][]*diffLayer, 2000)
		l.layers = make(map[common.Hash]struct{})

		// Apply the layers from bottom to top
		for i := len(layers) - 1; i >= 0; i-- {
			switch diff := layers[i].(type) {
			case *diskLayer:
				continue
			case *diffLayer:
				l.addAccount(diff)
				l.addStorage(diff)
			}
		}
	}

	{ // setup descendant mapping
		var (
			current     = head
			layers      = make(map[common.Hash]Snapshot)
			descendants = make(map[common.Hash]map[common.Hash]struct{})
		)
		for {
			hash := current.Root()
			layers[hash] = current

			// Traverse the ancestors (diff only) of the current layer and link them
			for h := range collectDiffLayerAncestors(current) {
				subset := descendants[h]
				if subset == nil {
					subset = make(map[common.Hash]struct{})
					descendants[h] = subset
				}
				subset[hash] = struct{}{}
			}
			parent := current.Parent()
			if parent == nil {
				break
			}
			current = parent
		}
		l.descendants = descendants
	}

	return l
}

func (l *Lookup) addAccount(diff *diffLayer) {
	defer func(now time.Time) {
		lookupAddLayerAccountTimer.UpdateSince(now)
	}(time.Now())
	//avgSize := 0
	for accountHash, _ := range diff.accountData {
		subset := l.stateToLayerAccount[accountHash]
		if subset == nil {
			subset = getSlice()
			l.stateToLayerAccount[accountHash] = subset
		}
		l.stateToLayerAccount[accountHash] = append(l.stateToLayerAccount[accountHash], diff)
		//avgSize += len(l.stateToLayerAccount[accountHash])
	}
}

func (l *Lookup) removeAccount(diff *diffLayer) error {
	diffRoot := diff.Root()
	defer func(now time.Time) {
		lookupRemoveLayerAccountTimer.UpdateSince(now)
	}(time.Now())
	for accountHash, _ := range diff.accountData {
		var (
			subset []*diffLayer
			exists bool
			found  bool
		)
		if subset, exists = l.stateToLayerAccount[accountHash]; exists {
			if subset == nil {
				returnSlice(subset)
				delete(l.stateToLayerAccount, accountHash)
				continue
			}
		} else {
			//TODO if error, this happens sometimes
			continue
			//log.Error("unknown account addr hash %s", accountHash)
		}

		//appendIndex := 0
		for j := 0; j < len(subset); j++ {
			if subset[j].Root() == diffRoot {
				subset[j] = nil
				if j == 0 {
					//appendIndex += 1
					subset = subset[1:] // TODO what if the underlying slice is held forever?
				} else {
					//appendIndex += j + 1
					//subset = append(subset[:j], subset[j+1:]...)
					copy(subset[j:], subset[j+1:])
					subset = subset[:len(subset)-1]
				}
				//subset[j] = nil
				//copy(subset[j:], subset[j+1:])
				//subset = subset[:len(subset)-1]

				//if j == 0 {
				//	subset = subset[1:] // TODO what if the underlying slice is held forever?
				//} else {
				//	subset = append(subset[:j], subset[j+1:]...)
				//}
				found = true
				break
			}
		}
		//lookupAccountAppendIndexGauge.Update(int64(appendIndex / len(subset)))
		if !found {
			// deleted flattened layers
			continue
			log.Error("failed to delete lookup %s", accountHash)
		}
		if len(subset) == 0 {
			returnSlice(subset)
			delete(l.stateToLayerAccount, accountHash)
		} else {
			l.stateToLayerAccount[accountHash] = subset
		}
	}
	return nil
}

func (l *Lookup) addStorage(diff *diffLayer) {
	defer func(now time.Time) {
		lookupAddLayerStorageTimer.UpdateSince(now)
	}(time.Now())
	//avgFirstSize := 0
	//avgSecondSize := 0

	for accountHash, slots := range diff.storageData {
		subset := l.stateToLayerStorage[accountHash]
		if subset == nil {
			subset = make(map[common.Hash][]*diffLayer, 16)
			//subset = getSliceMap()
			l.stateToLayerStorage[accountHash] = subset
		}
		//avgFirstSize += len(subset)
		for storageHash := range slots {
			subset[storageHash] = append(subset[storageHash], diff)
			//avgSecondSize += len(subset[storageHash])
		}
	}
}

func (l *Lookup) removeStorage(diff *diffLayer) error {
	diffRoot := diff.Root()

	defer func(now time.Time) {
		lookupRemoveLayerStorageTimer.UpdateSince(now)
	}(time.Now())
	for accountHash, slots := range diff.storageData {
		var (
			subset map[common.Hash][]*diffLayer
			exist  bool
		)
		if subset, exist = l.stateToLayerStorage[accountHash]; exist {
			if subset == nil {
				//returnMapToPool(subset)
				delete(l.stateToLayerStorage, accountHash)
				continue
				//TODO slice pool
			}
		}

		//appendIndex := 0
		for storageHash := range slots {
			var (
				slotSubset []*diffLayer
				slotExists bool
			)
			if slotSubset, slotExists = subset[storageHash]; slotExists {
				if slotSubset == nil {
					delete(subset, storageHash)
					continue
					//log.Error("unknown account addr hash %s", storageHash)
				}
			}

			var found bool
			for j := 0; j < len(slotSubset); j++ {
				if slotSubset[j].Root() == diffRoot {
					slotSubset[j] = nil
					if j == 0 {
						slotSubset = slotSubset[1:] // TODO what if the underlying slice is held forever?
					} else {
						//appendIndex += j
						//slotSubset = append(slotSubset[:j], slotSubset[j+1:]...)
						copy(slotSubset[j:], slotSubset[j+1:])
						slotSubset = slotSubset[:len(slotSubset)-1]
					}
					//if j == 0 {
					//	slotSubset = slotSubset[1:] // TODO what if the underlying slice is held forever?
					//} else {
					//	slotSubset = append(slotSubset[:j], slotSubset[j+1:]...)
					//}
					found = true
					break
				}
			}
			if !found {
				continue
				log.Error("failed to delete lookup %s", storageHash)
			}
			if len(slotSubset) == 0 {
				delete(subset, storageHash)
			} else {
				subset[storageHash] = slotSubset
			}
		}

		if len(subset) == 0 {
			//returnMapToPool(subset)
			delete(l.stateToLayerStorage, accountHash)
		}
	}
	return nil
}

//
//// addLayer traverses all the dirty state within the given diff layer and links
//// them into the lookup set.
//func (l *Lookup) addLayer(diff *diffLayer) {
//	defer func(now time.Time) {
//		lookupAddLayerTimer.UpdateSince(now)
//		lookupAddLayerCounter.Mark(1)
//	}(time.Now())
//	layerIDCounter++
//}
//// removeLayer traverses all the dirty state within the given diff layer and
//// unlinks them from the lookup set.
//func (l *Lookup) removeLayer(diff *diffLayer) error {
//	defer func(now time.Time) {
//		lookupRemoveLayerTimer.UpdateSince(now)
//		lookupRemoveLayerCounter.Mark(1)
//	}(time.Now())
//	layerIDRemoveCounter++
//
//	diffRoot := diff.Root()
//
//	var wg sync.WaitGroup
//	wg.Add(2)
//
//	wg.Wait()
//	return nil
//}

func (l *Lookup) addDescendant(topDiffLayer Snapshot) {
	defer func(now time.Time) {
		lookupAddDescendantTimer.UpdateSince(now)
	}(time.Now())

	// Link the new layer into the descendents set
	// TODO parallel
	//var (
	//	workers errgroup.Group
	//)
	//
	//workers.SetLimit(3)

	for h := range collectDiffLayerAncestors(topDiffLayer) {
		//workers.Go(func() error {
		//	lock := l.descendantsLock.getLock(h)
		//	lock.Lock()
		subset := l.descendants[h]
		if subset == nil {
			subset = make(map[common.Hash]struct{})
			l.descendants[h] = subset
		}
		subset[topDiffLayer.Root()] = struct{}{}
		//lock.Unlock()

		//return nil
		//}
	}
}

func (l *Lookup) removeDescendant(bottomDiffLayer Snapshot) {
	defer func(now time.Time) {
		lookupRemoveDescendantTimer.UpdateSince(now)
	}(time.Now())

	//lock := l.descendantsLock.getLock(bottomDiffLayer.Root())
	//lock.Lock()
	delete(l.descendants, bottomDiffLayer.Root())
	//lock.Unlock()
}

func (l *Lookup) isDescendant(state common.Hash, ancestor common.Hash) bool {
	//lock := l.descendantsLock.getLock(ancestor)
	//lock.Lock()
	subset := l.descendants[ancestor]
	if subset == nil {
		return false
	}
	_, ok := subset[state]

	//lock.Unlock()
	return ok
}

func (l *Lookup) AddSnapshot(diff *diffLayer) {
	defer func(now time.Time) {
		lookupAddSnapshotTimer.UpdateSince(now)
		lookupAddSnapshotCounter.Mark(1)
	}(time.Now())

	l.lock.Lock()
	defer l.lock.Unlock()
	l.layers[diff.Root()] = struct{}{}

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		l.addStorage(diff)
	}()
	go func() {
		defer wg.Done()
		l.addAccount(diff)
	}()
	go func() {
		defer wg.Done()
		l.addDescendant(diff)
	}()
	wg.Wait()
	lookupDescendantGauge.Update(int64(len(l.descendants)))
	lookupAccountGauge.Update(int64(len(l.stateToLayerAccount)))
	lookupStorageGauge.Update(int64(len(l.stateToLayerStorage)))
	lookupLayersGauge.Update(int64(len(l.layers)))
}

func (l *Lookup) RemoveSnapshot(diff *diffLayer) {
	defer func(now time.Time) {
		lookupRemoveSnapshotTimer.UpdateSince(now)
		lookupRemoveSnapshotCounter.Mark(1)
	}(time.Now())

	l.lock.Lock()
	defer l.lock.Unlock()

	diffRoot := diff.Root()
	if _, exist := l.layers[diffRoot]; exist {
		delete(l.layers, diffRoot)
	} else {
		l.removeDescendant(diff)
		return
	}

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()

		// remove layer storage cost time longer than remove account, so run it first
		l.removeStorage(diff)
	}()
	go func() {
		defer wg.Done()

		l.removeAccount(diff)
	}()
	go func() {
		defer wg.Done()

		l.removeDescendant(diff)
	}()

	wg.Wait()
	lookupDescendantGauge.Update(int64(len(l.descendants)))
	lookupAccountGauge.Update(int64(len(l.stateToLayerAccount)))
	lookupStorageGauge.Update(int64(len(l.stateToLayerStorage)))
}

func (l *Lookup) LookupAccount(accountAddrHash common.Hash, head common.Hash) Snapshot {
	defer func(now time.Time) {
		lookupLookupAccountTimer.UpdateSince(now)
	}(time.Now())

	//l.lock.RLock()
	//defer l.lock.RUnlock()

	defer func(now time.Time) {
		lookupLookupAccountNoLockTimer.UpdateSince(now)
	}(time.Now())

	// a ->  1 2 3 4 5 6 7 8 9 merge
	// 4 -> stale & flatten list[]
	//
	list, exists := l.stateToLayerAccount[accountAddrHash]
	if !exists {
		return nil
	}

	// Traverse the list in reverse order to find the first entry that either
	// matches the specified head or is a descendant of it.
	for i := len(list) - 1; i >= 0; i-- {
		if list[i].Root() == head || l.isDescendant(head, list[i].Root()) {
			return list[i]
		}
	}
	return nil
}

func (l *Lookup) LookupStorage(accountAddrHash common.Hash, slot common.Hash, head common.Hash) Snapshot {
	defer func(now time.Time) {
		lookupLookupStorageTimer.UpdateSince(now)
	}(time.Now())

	//l.lock.RLock()
	//defer l.lock.RUnlock()

	defer func(now time.Time) {
		lookupStorageNoLockTimer.UpdateSince(now)
	}(time.Now())

	subset, exists := l.stateToLayerStorage[accountAddrHash]
	if !exists {
		return nil
	}

	list, exists := subset[slot]
	if !exists {
		return nil
	}

	// Traverse the list in reverse order to find the first entry that either
	// matches the specified head or is a descendant of it.
	for i := len(list) - 1; i >= 0; i-- {
		if list[i].Root() == head || l.isDescendant(head, list[i].Root()) {
			return list[i]
		}
	}
	return nil
}
