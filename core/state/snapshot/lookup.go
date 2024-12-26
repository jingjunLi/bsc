package snapshot

import (
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"golang.org/x/sync/errgroup"
)

// slicePool is a shared pool of hash slice, for reducing the GC pressure.
var slicePool = sync.Pool{
	New: func() interface{} {
		slice := make([]Snapshot, 0, 16) // Pre-allocate a slice with a reasonable capacity.
		return &slice
	},
}

// getSlice obtains the hash slice from the shared pool.
func getSlice() []Snapshot {
	slice := *slicePool.Get().(*[]Snapshot)
	slice = slice[:0]
	return slice
}

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
	stateToLayerAccount map[common.Hash][]Snapshot
	stateToLayerStorage map[common.Hash]map[common.Hash][]Snapshot
	descendants         map[common.Hash]map[common.Hash]struct{}

	lock sync.RWMutex
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
		l.stateToLayerAccount = make(map[common.Hash][]Snapshot)
		l.stateToLayerStorage = make(map[common.Hash]map[common.Hash][]Snapshot)

		// Apply the layers from bottom to top
		for i := len(layers) - 1; i >= 0; i-- {
			switch diff := layers[i].(type) {
			case *diskLayer:
				continue
			case *diffLayer:
				l.addLayer(diff)
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

func (l *Lookup) isDescendant(state common.Hash, ancestor common.Hash) bool {
	subset := l.descendants[ancestor]
	if subset == nil {
		return false
	}
	_, ok := subset[state]
	return ok
}

var layerIDCounter int
var layerIDRemoveCounter int

// addLayer traverses all the dirty state within the given diff layer and links
// them into the lookup set.
func (l *Lookup) addLayer(diff *diffLayer) {
	defer func(now time.Time) {
		lookupAddLayerTimer.UpdateSince(now)
	}(time.Now())
	layerIDCounter++

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		for accountHash, _ := range diff.accountData {
			l.stateToLayerAccount[accountHash] = append(l.stateToLayerAccount[accountHash], diff)
		}
		wg.Done()
	}()

	go func() {
		for accountHash, slots := range diff.storageData {
			subset := l.stateToLayerStorage[accountHash]
			if subset == nil {
				subset = make(map[common.Hash][]Snapshot)
				l.stateToLayerStorage[accountHash] = subset
			}
			for storageHash := range slots {
				subset[storageHash] = append(subset[storageHash], diff)
			}
		}
		wg.Done()
	}()

	wg.Wait()
}

// removeLayer traverses all the dirty state within the given diff layer and
// unlinks them from the lookup set.
func (l *Lookup) removeLayer(diff *diffLayer) error {
	defer func(now time.Time) {
		lookupRemoveLayerTimer.UpdateSince(now)
	}(time.Now())
	layerIDRemoveCounter++

	diffRoot := diff.Root()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for accountHash, _ := range diff.accountData {
			subset := l.stateToLayerAccount[accountHash]
			if subset == nil {
				//TODO if error, this happens sometimes
				return
				//log.Error("unknown account addr hash %s", accountHash)
			}
			var found bool
			for j := 0; j < len(subset); j++ {
				if subset[j].Root() == diffRoot {
					if j == 0 {
						subset = subset[1:] // TODO what if the underlying slice is held forever?
					} else {
						subset = append(subset[:j], subset[j+1:]...)
					}
					found = true
					break
				}
			}
			if !found {
				return
				log.Error("failed to delete lookup %s", accountHash)
			}
			if len(subset) == 0 {
				delete(l.stateToLayerAccount, accountHash)
			} else {
				l.stateToLayerAccount[accountHash] = subset
			}
		}
	}()

	go func() {
		defer wg.Done()
		for accountHash, slots := range diff.storageData {
			subset := l.stateToLayerStorage[accountHash]
			if subset == nil {
				subset = make(map[common.Hash][]Snapshot)
				l.stateToLayerStorage[accountHash] = subset
			}
			for storageHash := range slots {
				slotSubset := subset[storageHash]
				if slotSubset == nil {
					return
					log.Error("unknown account addr hash %s", storageHash)
				}
				var found bool
				for j := 0; j < len(slotSubset); j++ {
					if slotSubset[j].Root() == diffRoot {
						if j == 0 {
							slotSubset = slotSubset[1:] // TODO what if the underlying slice is held forever?
						} else {
							slotSubset = append(slotSubset[:j], slotSubset[j+1:]...)
						}
						found = true
						break
					}
				}
				if !found {
					return
					log.Error("failed to delete lookup %s", storageHash)
				}
				if len(slotSubset) == 0 {
					delete(l.stateToLayerStorage, storageHash)
				} else {
					subset[storageHash] = slotSubset
				}
			}
		}

	}()

	wg.Wait()
	return nil
}

// diffAncestors returns all the ancestors of the specific layer (disk layer
// is not included).
func diffAncestors(layer Snapshot) map[common.Hash]struct{} {
	set := make(map[common.Hash]struct{})
	for {
		parent := layer.Parent()
		if parent == nil {
			break
		}
		if _, ok := parent.(*diskLayer); ok {
			break
		}
		set[parent.Root()] = struct{}{}
		layer = parent
	}
	return set
}

func (l *Lookup) addDescendant(topDiffLayer Snapshot) {
	defer func(now time.Time) {
		lookupAddDescendantTimer.UpdateSince(now)
	}(time.Now())

	// Link the new layer into the descendents set
	// TODO parallel
	var workers errgroup.Group
	workers.SetLimit(2)

	for h := range diffAncestors(topDiffLayer) {
		workers.Go(func() error {
			subset := l.descendants[h]
			if subset == nil {
				subset = make(map[common.Hash]struct{})
				l.descendants[h] = subset
			}
			subset[topDiffLayer.Root()] = struct{}{}
			return nil
		})
	}
}

func (l *Lookup) removeDescendant(bottomDiffLayer Snapshot) {
	defer func(now time.Time) {
		lookupRemoveDescendantTimer.UpdateSince(now)
	}(time.Now())

	delete(l.descendants, bottomDiffLayer.Root())
}

func (l *Lookup) AddSnapshot(diff *diffLayer) {
	l.lock.Lock()
	defer l.lock.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		l.addLayer(diff)
		wg.Done()
	}()
	go func() {
		l.addDescendant(diff)
		wg.Done()
	}()
	wg.Wait()
}

func (l *Lookup) RemoveSnapshot(diff *diffLayer) {
	l.lock.Lock()
	defer l.lock.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		l.removeLayer(diff)
		wg.Done()
	}()
	go func() {
		l.removeDescendant(diff)
		wg.Done()
	}()

	wg.Wait()
}

func (l *Lookup) LookupAccount(accountAddrHash common.Hash, head common.Hash) Snapshot {
	l.lock.RLock()
	defer l.lock.RUnlock()

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
	l.lock.RLock()
	defer l.lock.RUnlock()

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
