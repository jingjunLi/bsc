// Copyright 2022 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package pathdb

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"
)

// diffLayer represents a collection of modifications made to the in-memory tries
// along with associated state changes after running a block on top.
//
// The goal of a diff layer is to act as a journal, tracking recent modifications
// made to the state, that have not yet graduated into a semi-immutable state.
/*
diffLayer 表示 内存中 tries 的一系列修改, diff layer 的目标是与 journal 相似, 追踪最近修改的 state; 不保证进入半不变状态 ?
diffLayer 相当于保存在内存中的缓存, 类似 journal; 所以即使丢失 也可以进行重建;

1) id 用途 ? the state id of the layer ?
2) nodes 用途 ? Cached trie nodes indexed by owner and path ?
	nodes trie nodes , 会传给 diskLayer & nodecache
	owner(addr) -> map[string]*trienode.Node 的映射 ?
	nodes 是如何赋值的 ? 通过下面的调用方式 一路传下来:MergedNodeSet 转换而成;
	(db *Database) Update -> (db *Database) Update -> (tree *layerTree) add(trienode.MergedNodeSet) -> (dl *diffLayer) update (nodes)
3) parent & lock: lock 保护 parent ? 为什么只保护 parent, 存在并发读写 ?
4) states:存储了diffLayer这一层改变的account在改变前它的数据是什么样的。这个数据用来构建freezer的数据时使用。这是和snapshot不同的地方。
---
核心的功能:

*/
type diffLayer struct {
	// Immutables
	root   common.Hash                               // Root hash to which this layer diff belongs to
	id     uint64                                    // Corresponding state id
	block  uint64                                    // Associated block number
	nodes  map[common.Hash]map[string]*trienode.Node // Cached trie nodes indexed by owner and path
	states *triestate.Set                            // Associated state change set for building history
	memory uint64                                    // Approximate guess as to how much memory we use

	parent layer        // Parent layer modified by this one, never nil, **can be changed**
	lock   sync.RWMutex // Lock used to protect parent
}

// newDiffLayer creates a new diff layer on top of an existing layer.
/*
基于 existing layer 创建一个新的 diff layer ? 新创建一个 diffLayer, 并且 其 parent 指向 传入的 parent 参数;
*/
func newDiffLayer(parent layer, root common.Hash, id uint64, block uint64, nodes map[common.Hash]map[string]*trienode.Node, states *triestate.Set) *diffLayer {
	var (
		size  int64
		count int
	)
	dl := &diffLayer{
		root:   root,
		id:     id,
		block:  block,
		nodes:  nodes,
		states: states,
		parent: parent,
	}
	for _, subset := range nodes {
		for path, n := range subset {
			dl.memory += uint64(n.Size() + len(path))
			size += int64(len(n.Blob) + len(path))
		}
		count += len(subset)
	}
	if states != nil {
		dl.memory += uint64(states.Size())
	}
	dirtyWriteMeter.Mark(size)
	diffLayerNodesMeter.Mark(int64(count))
	diffLayerBytesMeter.Mark(int64(dl.memory))
	log.Debug("Created new diff layer", "id", id, "block", block, "nodes", count, "size", common.StorageSize(dl.memory), "root", dl.root)
	return dl
}

// rootHash implements the layer interface, returning the root hash of
// corresponding state.
func (dl *diffLayer) rootHash() common.Hash {
	return dl.root
}

// stateID implements the layer interface, returning the state id of the layer.
func (dl *diffLayer) stateID() uint64 {
	return dl.id
}

// parentLayer implements the layer interface, returning the subsequent
// layer of the diff layer.
func (dl *diffLayer) parentLayer() layer {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	return dl.parent
}

// node retrieves the node with provided node information. It's the internal
// version of Node function with additional accessed layer tracked. No error
// will be returned if node is not found.
/*
查询接口, 给定 node 信息;
*/
func (dl *diffLayer) node(owner common.Hash, path []byte, hash common.Hash, depth int) ([]byte, error) {
	// Hold the lock, ensure the parent won't be changed during the
	// state accessing.
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	// If the trie node is known locally, return it
	subset, ok := dl.nodes[owner]
	if ok {
		n, ok := subset[string(path)]
		if ok {
			// If the trie node is not hash matched, or marked as removed,
			// bubble up an error here. It shouldn't happen at all.
			if n.Hash != hash {
				dirtyFalseMeter.Mark(1)
				log.Error("Unexpected trie node in diff layer", "owner", owner, "path", path, "expect", hash, "got", n.Hash)
				return nil, newUnexpectedNodeError("diff", hash, n.Hash, owner, path, n.Blob)
			}
			dirtyHitMeter.Mark(1)
			dirtyNodeHitDepthHist.Update(int64(depth))
			dirtyReadMeter.Mark(int64(len(n.Blob)))
			return n.Blob, nil
		}
	}
	// Trie node unknown to this layer, resolve from parent
	if diff, ok := dl.parent.(*diffLayer); ok {
		return diff.node(owner, path, hash, depth+1)
	}
	// Failed to resolve through diff layers, fallback to disk layer
	return dl.parent.Node(owner, path, hash)
}

// Node implements the layer interface, retrieving the trie node blob with the
// provided node information. No error will be returned if the node is not found.
func (dl *diffLayer) Node(owner common.Hash, path []byte, hash common.Hash) ([]byte, error) {
	return dl.node(owner, path, hash, 0)
}

// update implements the layer interface, creating a new layer on top of the
// existing layer tree with the specified data items.
func (dl *diffLayer) update(root common.Hash, id uint64, block uint64, nodes map[common.Hash]map[string]*trienode.Node, states *triestate.Set) *diffLayer {
	return newDiffLayer(dl, root, id, block, nodes, states)
}

// persist flushes the diff layer and all its parent layers to disk layer.
/*
 persist 这是一个递归调用, 将 所有的 parent 的 diff layer 持久化 TODO(优化), 递归不高效
*/
func (dl *diffLayer) persist(force bool) (layer, error) {
	if parent, ok := dl.parentLayer().(*diffLayer); ok {
		// Hold the lock to prevent any read operation until the new
		// parent is linked correctly.
		dl.lock.Lock()

		// The merging of diff layers starts at the bottom-most layer,
		// therefore we recurse down here, flattening on the way up
		// (diffToDisk).
		result, err := parent.persist(force)
		if err != nil {
			dl.lock.Unlock()
			return nil, err
		}
		dl.parent = result
		dl.lock.Unlock()
	}
	return diffToDisk(dl, force)
}

// diffToDisk merges a bottom-most diff into the persistent disk layer underneath
// it. The method will panic if called onto a non-bottom-most diff layer.
func diffToDisk(layer *diffLayer, force bool) (layer, error) {
	disk, ok := layer.parentLayer().(*diskLayer)
	if !ok {
		panic(fmt.Sprintf("unknown layer type: %T", layer.parentLayer()))
	}
	return disk.commit(layer, force)
}
