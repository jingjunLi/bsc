package snapshot

import (
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

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

	for accountHash, _ := range diff.accountData {
		l.stateToLayerAccount[accountHash] = append(l.stateToLayerAccount[accountHash], diff)
	}

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
}

// removeLayer traverses all the dirty state within the given diff layer and
// unlinks them from the lookup set.
func (l *Lookup) removeLayer(diff *diffLayer) error {
	defer func(now time.Time) {
		lookupRemoveLayerTimer.UpdateSince(now)
	}(time.Now())
	layerIDRemoveCounter++

	diffRoot := diff.Root()
	for accountHash, _ := range diff.accountData {
		subset := l.stateToLayerAccount[accountHash]
		if subset == nil {
			return fmt.Errorf("unknown account addr hash %s", accountHash)
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
			return fmt.Errorf("failed to delete lookup %s", accountHash)
		}
		if len(subset) == 0 {
			delete(l.stateToLayerAccount, accountHash)
		} else {
			l.stateToLayerAccount[accountHash] = subset
		}
	}

	for accountHash, slots := range diff.storageData {
		subset := l.stateToLayerStorage[accountHash]
		if subset == nil {
			subset = make(map[common.Hash][]Snapshot)
			l.stateToLayerStorage[accountHash] = subset
		}
		for storageHash := range slots {
			slotSubset := subset[storageHash]
			if slotSubset == nil {
				return fmt.Errorf("unknown account addr hash %s", storageHash)
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
				return fmt.Errorf("failed to delete lookup %s", storageHash)
			}
			if len(slotSubset) == 0 {
				delete(l.stateToLayerStorage, storageHash)
			} else {
				subset[storageHash] = slotSubset
			}
		}
	}

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
	for h := range diffAncestors(topDiffLayer) {
		subset := l.descendants[h]
		if subset == nil {
			subset = make(map[common.Hash]struct{})
			l.descendants[h] = subset
		}
		subset[topDiffLayer.Root()] = struct{}{}
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
	l.addLayer(diff)
	l.addDescendant(diff)
}

func (l *Lookup) RemoveSnapshot(diff *diffLayer) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.removeLayer(diff)
	l.removeDescendant(diff)
}

func (l *Lookup) LookupAccount(accountAddrHash common.Hash, head common.Hash) Snapshot {
	l.lock.RLock()
	defer l.lock.RUnlock()

	list, exists := l.stateToLayerAccount[accountAddrHash]
	if !exists {
		//log.Info("lookupAccount not exist", "acc", accountAddrHash, "head", head)
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
		//log.Info("LookupStorage not exist", "acc", accountAddrHash, "head", head)
		return nil
	}

	list := subset[slot]

	// Traverse the list in reverse order to find the first entry that either
	// matches the specified head or is a descendant of it.
	for i := len(list) - 1; i >= 0; i-- {
		if list[i].Root() == head || l.isDescendant(head, list[i].Root()) {
			return list[i]
		}
	}
	return nil
}
