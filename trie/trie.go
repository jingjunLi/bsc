// Copyright 2014 The go-ethereum Authors
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

// Package trie implements Merkle Patricia Tries.
package trie

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/triedb/database"
)

// Trie is a Merkle Patricia Trie. Use New to create a trie that sits on
// top of a database. Whenever trie performs a commit operation, the generated
// nodes will be gathered and returned in a set. Once the trie is committed,
// it's not usable anymore. Callers have to re-create the trie with new root
// based on the updated trie database.
//
// Trie is not safe for concurrent use.
/*
Trie is a Merkle Patricia Trie. 1) 使用 New 来创建 trie, 基于 database; 档 trie 执行 commit 操作时, 生成的 nodes 会聚集返回一个 set;
一旦 trie committed, 它就变成不可用 ?
对外提供的接口:
1) New

trie的结构， root包含了当前的root节点， db是后端的KV存储，trie的结构最终都是需要通过KV的形式存储到数据库里面去，然后启动的时候是需要从数据库里面加载的。
originalRoot 启动加载的时候的hash值，通过这个hash值可以在数据库里面恢复出整颗的trie树。cachegen字段指示了当前Trie树的cache时代，
每次调用Commit操作的时候，会增加Trie树的cache时代。 cache时代会被附加在node节点上面，如果当前的cache时代 - cachelimit参数 大于node的cache时代，
那么node会从cache里面卸载，以便节约内存。 其实这就是缓存更新的LRU算法， 如果一个缓存在多久没有被使用，那么就从缓存里面移除，以节约内存空间。

1) root : 当前的 root 节点

核心内容:
1) 理解 trie 的结构, 各种结点的定义和使用;
2) 理解 trie 的操作, 各种增删改查;Get, Update, Delete, Commit
Hash() : 返回当前 trie 的 root hash
Commit() : 提交 trie 的修改
*/
type Trie struct {
	root  node
	owner common.Hash

	// Flag whether the commit operation is already performed. If so the
	// trie is not usable(latest states is invisible).
	committed bool

	// Keep track of the number leaves which have been inserted since the last
	// hashing operation. This number will not directly map to the number of
	// actually unhashed nodes.
	unhashed int

	// reader is the handler trie can retrieve nodes from.
	reader *trieReader

	// tracer is the tool to track the trie changes.
	// It will be reset after each commit operation.
	tracer *tracer
}

// newFlag returns the cache flag value for a newly created node.
func (t *Trie) newFlag() nodeFlag {
	return nodeFlag{dirty: true}
}

// Copy returns a copy of Trie.
func (t *Trie) Copy() *Trie {
	return &Trie{
		root:      t.root,
		owner:     t.owner,
		committed: t.committed,
		unhashed:  t.unhashed,
		reader:    t.reader,
		tracer:    t.tracer.copy(),
	}
}

// New creates the trie instance with provided trie id and the read-only
// database. The state specified by trie id must be available, otherwise
// an error will be returned. The trie root specified by trie id can be
// zero hash or the sha3 hash of an empty string, then trie is initially
// empty, otherwise, the root node must be present in database or returns
// a MissingNodeError if not.
/*
创建 Trie ?
*/
func New(id *ID, db database.Database) (*Trie, error) {
	reader, err := newTrieReader(id.StateRoot, id.Owner, db)
	if err != nil {
		return nil, err
	}
	trie := &Trie{
		owner:  id.Owner,
		reader: reader,
		tracer: newTracer(),
	}
	if id.Root != (common.Hash{}) && id.Root != types.EmptyRootHash {
		rootnode, err := trie.resolveAndTrack(id.Root[:], nil)
		if err != nil {
			return nil, err
		}
		trie.root = rootnode
	}
	return trie, nil
}

// NewEmpty is a shortcut to create empty tree. It's mostly used in tests.
func NewEmpty(db database.Database) *Trie {
	tr, _ := New(TrieID(types.EmptyRootHash), db)
	return tr
}

// MustNodeIterator is a wrapper of NodeIterator and will omit any encountered
// error but just print out an error message.
func (t *Trie) MustNodeIterator(start []byte) NodeIterator {
	it, err := t.NodeIterator(start)
	if err != nil {
		log.Error("Unhandled trie error in Trie.NodeIterator", "err", err)
	}
	return it
}

// NodeIterator returns an iterator that returns nodes of the trie. Iteration starts at
// the key after the given start key.
func (t *Trie) NodeIterator(start []byte) (NodeIterator, error) {
	// Short circuit if the trie is already committed and not usable.
	if t.committed {
		return nil, ErrCommitted
	}
	return newNodeIterator(t, start), nil
}

// MustGet is a wrapper of Get and will omit any encountered error but just
// print out an error message.
func (t *Trie) MustGet(key []byte) []byte {
	res, err := t.Get(key)
	if err != nil {
		log.Error("Unhandled trie error in Trie.Get", "err", err)
	}
	return res
}

// Get returns the value for key stored in the trie.
// The value bytes must not be modified by the caller.
//
// If the requested node is not present in trie, no error will be returned.
// If the trie is corrupted, a MissingNodeError is returned.
/*
1) Trie::root 从 root 节点开始查找;
2) key 是 SHA 之后的 字节数组, 转换为 Hex Key
*/
func (t *Trie) Get(key []byte) ([]byte, error) {
	// Short circuit if the trie is already committed and not usable.
	if t.committed {
		return nil, ErrCommitted
	}
	value, newroot, didResolve, err := t.get(t.root, keybytesToHex(key), 0)
	if err == nil && didResolve {
		t.root = newroot
	}
	return value, err
}

/*
1) valueNode
2) shortNode
3) fullNode
4) hashNode
---
1) origNode: 当前查找的起始 node 位置
2) key: 输入要查找的数据的 hash
3) pos: 当前hash匹配到第几位

didResolve 如何理解 ?

起始输入参数: t.root, key, 0
---
从根节点开始搜寻与搜索路径内容一致的路径；
1. 若当前节点为叶子节点，存储的内容是数据项的内容，且搜索路径的内容与叶子节点的key一致，则表示找到该节点；反之则表示该节点在树中不存在。
2. 若当前节点为扩展节点，且存储的内容是哈希索引，则利用哈希索引从数据库中加载该节点，再将搜索路径作为参数，对新解析出来的节点递归地调用查找函数。
3. 若当前节点为扩展节点，存储的内容是另外一个节点的引用，且当前节点的key是搜索路径的前缀，则将搜索路径减去当前节点的key，将剩余的搜索路径作为参数，对其子节点递归地调用查找函数；若当前节点的key不是搜索路径的前缀，表示该节点在树中不存在。
4. 若当前节点为分支节点，若搜索路径为空，则返回分支节点的存储内容；反之利用搜索路径的第一个字节选择分支节点的孩子节点，将剩余的搜索路径作为参数递归地调用查找函数。
*/
func (t *Trie) get(origNode node, key []byte, pos int) (value []byte, newnode node, didResolve bool, err error) {
	switch n := (origNode).(type) {
	case nil:
		return nil, nil, false, nil
	case valueNode:
		// 1. 若当前节点为叶子节点，存储的内容是数据项的内容，且搜索路径的内容与叶子节点的key一致，则表示找到该节点；反之则表示该节点在树中不存在。
		// 走到这里, 有可能是 找到了, 然后上层递归的返回 valueNode ???
		return n, n, false, nil
	case *shortNode:
		// 叶子节点 / 扩展节点
		/*
			搜索路径的内容与叶子节点的key 是否一致 ? 如果不一致, key 不存在: 直接返回 value(nil), didResolve false ?
		*/
		if len(key)-pos < len(n.Key) || !bytes.Equal(n.Key, key[pos:pos+len(n.Key)]) {
			// key not found in trie
			return nil, n, false, nil
		}
		value, newnode, didResolve, err = t.get(n.Val, key, pos+len(n.Key))
		if err == nil && didResolve {
			n = n.copy()
			n.Val = newnode
		}
		return value, n, didResolve, err
	case *fullNode:
		// 4. 若当前节点为分支节点，若搜索路径为空，则返回分支节点的存储内容；反之利用搜索路径的第一个字节选择分支节点的孩子节点，将剩余的搜索路径作为参数递归地调用查找函数。
		value, newnode, didResolve, err = t.get(n.Children[key[pos]], key, pos+1)
		if err == nil && didResolve {
			n = n.copy()
			n.Children[key[pos]] = newnode
		}
		return value, n, didResolve, err
	case hashNode:
		/*
			hashNode ??  其调用方 是 扩展节点
			通过 shortNode.Val 指向下一个 node;

			1) didResolve 为 true 表示, 则递归调用的上层 都为 true.
			从 底层存储读取 key 对应的 -> value, 读取 成功.
			持久化 并且 ?? 将另外一个节点的引用 读取出来, 继续查找.

			3. 若当前节点为扩展节点，存储的内容是另外一个节点的引用，且当前节点的key是搜索路径的前缀，则将搜索路径减去当前节点的key，将剩余的搜索路径作为参数，
				对其子节点递归地调用查找函数；若当前节点的key不是搜索路径的前缀，表示该节点在树中不存在。
		*/
		child, err := t.resolveAndTrack(n, key[:pos])
		if err != nil {
			return nil, n, true, err
		}
		value, newnode, _, err := t.get(child, key, pos)
		return value, newnode, true, err
	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

// MustGetNode is a wrapper of GetNode and will omit any encountered error but
// just print out an error message.
func (t *Trie) MustGetNode(path []byte) ([]byte, int) {
	item, resolved, err := t.GetNode(path)
	if err != nil {
		log.Error("Unhandled trie error in Trie.GetNode", "err", err)
	}
	return item, resolved
}

// GetNode retrieves a trie node by compact-encoded path. It is not possible
// to use keybyte-encoding as the path might contain odd nibbles.
//
// If the requested node is not present in trie, no error will be returned.
// If the trie is corrupted, a MissingNodeError is returned.
/*
GetNode 方法与 Get 方法流程几乎一样，除了它采用 COMPACT 对 key 编码去查找 node。
通过 compact-encoded path 来检索 trie node;
*/
func (t *Trie) GetNode(path []byte) ([]byte, int, error) {
	// Short circuit if the trie is already committed and not usable.
	if t.committed {
		return nil, 0, ErrCommitted
	}
	item, newroot, resolved, err := t.getNode(t.root, compactToHex(path), 0)
	if err != nil {
		return nil, resolved, err
	}
	if resolved > 0 {
		t.root = newroot
	}
	if item == nil {
		return nil, resolved, nil
	}
	return item, resolved, nil
}

func (t *Trie) getNode(origNode node, path []byte, pos int) (item []byte, newnode node, resolved int, err error) {
	// If non-existent path requested, abort
	if origNode == nil {
		return nil, nil, 0, nil
	}
	// If we reached the requested path, return the current node
	if pos >= len(path) {
		// Although we most probably have the original node expanded, encoding
		// that into consensus form can be nasty (needs to cascade down) and
		// time consuming. Instead, just pull the hash up from disk directly.
		var hash hashNode
		if node, ok := origNode.(hashNode); ok {
			hash = node
		} else {
			hash, _ = origNode.cache()
		}
		if hash == nil {
			return nil, origNode, 0, errors.New("non-consensus node")
		}
		blob, err := t.reader.node(path, common.BytesToHash(hash))
		return blob, origNode, 1, err
	}
	// Path still needs to be traversed, descend into children
	switch n := (origNode).(type) {
	case valueNode:
		// Path prematurely ended, abort
		return nil, nil, 0, nil

	case *shortNode:
		if len(path)-pos < len(n.Key) || !bytes.Equal(n.Key, path[pos:pos+len(n.Key)]) {
			// Path branches off from short node
			return nil, n, 0, nil
		}
		item, newnode, resolved, err = t.getNode(n.Val, path, pos+len(n.Key))
		if err == nil && resolved > 0 {
			n = n.copy()
			n.Val = newnode
		}
		return item, n, resolved, err

	case *fullNode:
		item, newnode, resolved, err = t.getNode(n.Children[path[pos]], path, pos+1)
		if err == nil && resolved > 0 {
			n = n.copy()
			n.Children[path[pos]] = newnode
		}
		return item, n, resolved, err

	case hashNode:
		child, err := t.resolveAndTrack(n, path[:pos])
		if err != nil {
			return nil, n, 1, err
		}
		item, newnode, resolved, err := t.getNode(child, path, pos)
		return item, newnode, resolved + 1, err

	default:
		panic(fmt.Sprintf("%T: invalid node: %v", origNode, origNode))
	}
}

// MustUpdate is a wrapper of Update and will omit any encountered error but
// just print out an error message.
func (t *Trie) MustUpdate(key, value []byte) {
	if err := t.Update(key, value); err != nil {
		log.Error("Unhandled trie error in Trie.Update", "err", err)
	}
}

// Update associates key with value in the trie. Subsequent calls to
// Get will return value. If value has length zero, any existing value
// is deleted from the trie and calls to Get will return nil.
//
// The value bytes must not be modified by the caller while they are
// stored in the trie.
//
// If the requested node is not present in trie, no error will be returned.
// If the trie is corrupted, a MissingNodeError is returned.
/*
Update
1) insert
2) delete
*/
func (t *Trie) Update(key, value []byte) error {
	// Short circuit if the trie is already committed and not usable.
	if t.committed {
		return ErrCommitted
	}
	return t.update(key, value)
}

func (t *Trie) update(key, value []byte) error {
	t.unhashed++
	k := keybytesToHex(key)
	if len(value) != 0 {
		_, n, err := t.insert(t.root, nil, k, valueNode(value))
		if err != nil {
			return err
		}
		t.root = n
	} else {
		_, n, err := t.delete(t.root, nil, k)
		if err != nil {
			return err
		}
		t.root = n
	}
	return nil
}

/*
没有了 ??
Trie 树的 cache 管理。 还记得Trie树的结构里面有两个参数， 一个是cachegen,一个是cachelimit。这两个参数就是cache控制的参数。
Trie 树每一次调用 Commit 方法，会导致当前的cachegen增加1。
---
插入操作也是基于查找过程完成的，一个插入过程为：

 1. 根据4.1中描述的查找步骤，首先找到与新插入节点拥有最长相同路径前缀的节点，记为Node；
 2. 若该Node为分支节点：
    剩余的搜索路径不为空，则将新节点作为一个叶子节点插入到对应的孩子列表中；
    剩余的搜索路径为空（完全匹配），则将新节点的内容存储在分支节点的第17个孩子节点项中（Value）；
 3. 若该节点为叶子／扩展节点：
    3.1 剩余的搜索路径与当前节点的key一致，则把当前节点Val更新即可；
    3.2 剩余的搜索路径与当前节点的key不完全一致，则将叶子／扩展节点的孩子节点替换成分支节点，将新节点与当前节点key的共同前缀作为当前节点的key，
    将新节点与当前节点的孩子节点作为两个孩子插入到分支节点的孩子列表中，同时当前节点转换成了一个扩展节点（若新节点与当前节点没有共同前缀，则直接用生成的分支节点替换当前节点）；
 4. 若插入成功，则将被修改节点的dirty标志置为true，hash标志置空（之前的结果已经不可能用），且将节点的诞生标记更新为现在；
*/
func (t *Trie) insert(n node, prefix, key []byte, value node) (bool, node, error) {
	/*
		在 insert 方法的开头，检查键的长度是否为零，如果是，则表示在当前节点处结束。
		在这种情况下，如果当前节点是一个 valueNode 类型的节点，则直接比较其值与要插入的值是否相等，如果相等则不做任何修改，否则替换为要插入的值。
	*/
	if len(key) == 0 {
		if v, ok := n.(valueNode); ok {
			return !bytes.Equal(v, value.(valueNode)), value, nil
		}
		//  4. 若插入成功，则将被修改节点的dirty标志置为true，hash标志置空（之前的结果已经不可能用），且将节点的诞生标记更新为现在；
		return true, value, nil
	}
	switch n := n.(type) {
	case *shortNode:
		matchlen := prefixLen(key, n.Key)
		// If the whole key matches, keep this short node as is
		// and only update the value.
		// 3.1 剩余的搜索路径与当前节点的key一致，则把当前节点Val更新即可；
		if matchlen == len(n.Key) {
			// 找到了 ??
			dirty, nn, err := t.insert(n.Val, append(prefix, key[:matchlen]...), key[matchlen:], value)
			if !dirty || err != nil {
				return false, n, err
			}
			//  4. 若插入成功，则将被修改节点的dirty标志置为true，hash标志置空（之前的结果已经不可能用），且将节点的诞生标记更新为现在；
			return true, &shortNode{n.Key, nn, t.newFlag()}, nil
		}
		// Otherwise branch out at the index where they differ.
		// 3.2 剩余的搜索路径与当前节点的key不完全一致，则将叶子／扩展节点的孩子节点替换成分支节点，将新节点与当前节点key的共同前缀作为当前节点的key，
		branch := &fullNode{flags: t.newFlag()}
		var err error
		_, branch.Children[n.Key[matchlen]], err = t.insert(nil, append(prefix, n.Key[:matchlen+1]...), n.Key[matchlen+1:], n.Val)
		if err != nil {
			return false, nil, err
		}
		_, branch.Children[key[matchlen]], err = t.insert(nil, append(prefix, key[:matchlen+1]...), key[matchlen+1:], value)
		if err != nil {
			return false, nil, err
		}
		// Replace this shortNode with the branch if it occurs at index 0.
		if matchlen == 0 {
			return true, branch, nil
		}
		// New branch node is created as a child of the original short node.
		// Track the newly inserted node in the tracer. The node identifier
		// passed is the path from the root node.
		t.tracer.onInsert(append(prefix, key[:matchlen]...))

		// Replace it with a short node leading up to the branch.
		//  4. 若插入成功，则将被修改节点的dirty标志置为true，hash标志置空（之前的结果已经不可能用），且将节点的诞生标记更新为现在；
		return true, &shortNode{key[:matchlen], branch, t.newFlag()}, nil

	case *fullNode:
		/*
			若该Node为分支节点：
			剩余的搜索路径不为空，则将新节点作为一个叶子节点插入到对应的孩子列表中；
			剩余的搜索路径为空（完全匹配），则将新节点的内容存储在分支节点的第17个孩子节点项中（Value）；
		*/
		dirty, nn, err := t.insert(n.Children[key[0]], append(prefix, key[0]), key[1:], value)
		if !dirty || err != nil {
			return false, n, err
		}
		n = n.copy()
		n.flags = t.newFlag()
		n.Children[key[0]] = nn
		return true, n, nil

	case nil:
		// New short node is created and track it in the tracer. The node identifier
		// passed is the path from the root node. Note the valueNode won't be tracked
		// since it's always embedded in its parent.
		t.tracer.onInsert(prefix)

		return true, &shortNode{key, value, t.newFlag()}, nil

	case hashNode:
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and insert into it. This leaves all child nodes on
		// the path to the value in the trie.
		rn, err := t.resolveAndTrack(n, prefix)
		if err != nil {
			return false, nil, err
		}
		dirty, nn, err := t.insert(rn, prefix, key, value)
		if !dirty || err != nil {
			return false, rn, err
		}
		return true, nn, nil

	default:
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// MustDelete is a wrapper of Delete and will omit any encountered error but
// just print out an error message.
func (t *Trie) MustDelete(key []byte) {
	if err := t.Delete(key); err != nil {
		log.Error("Unhandled trie error in Trie.Delete", "err", err)
	}
}

// Delete removes any existing value for key from the trie.
//
// If the requested node is not present in trie, no error will be returned.
// If the trie is corrupted, a MissingNodeError is returned.
func (t *Trie) Delete(key []byte) error {
	// Short circuit if the trie is already committed and not usable.
	if t.committed {
		return ErrCommitted
	}
	t.unhashed++
	k := keybytesToHex(key)
	_, n, err := t.delete(t.root, nil, k)
	if err != nil {
		return err
	}
	t.root = n
	return nil
}

// delete returns the new root of the trie with key deleted.
// It reduces the trie to minimal form by simplifying
// nodes on the way up after deleting recursively.
func (t *Trie) delete(n node, prefix, key []byte) (bool, node, error) {
	switch n := n.(type) {
	case *shortNode:
		/*
			shortNode -> branchNode ?
		*/
		matchlen := prefixLen(key, n.Key)
		if matchlen < len(n.Key) {
			return false, n, nil // don't replace n on mismatch
		}
		if matchlen == len(key) {
			// The matched short node is deleted entirely and track
			// it in the deletion set. The same the valueNode doesn't
			// need to be tracked at all since it's always embedded.
			t.tracer.onDelete(prefix)

			return true, nil, nil // remove n entirely for whole matches
		}
		// The key is longer than n.Key. Remove the remaining suffix
		// from the subtrie. Child can never be nil here since the
		// subtrie must contain at least two other values with keys
		// longer than n.Key.
		dirty, child, err := t.delete(n.Val, append(prefix, key[:len(n.Key)]...), key[len(n.Key):])
		if !dirty || err != nil {
			return false, n, err
		}
		switch child := child.(type) {
		case *shortNode:
			// The child shortNode is merged into its parent, track
			// is deleted as well.
			t.tracer.onDelete(append(prefix, n.Key...))

			// Deleting from the subtrie reduced it to another
			// short node. Merge the nodes to avoid creating a
			// shortNode{..., shortNode{...}}. Use concat (which
			// always creates a new slice) instead of append to
			// avoid modifying n.Key since it might be shared with
			// other nodes.
			return true, &shortNode{concat(n.Key, child.Key...), child.Val, t.newFlag()}, nil
		default:
			return true, &shortNode{n.Key, child, t.newFlag()}, nil
		}

	case *fullNode:
		dirty, nn, err := t.delete(n.Children[key[0]], append(prefix, key[0]), key[1:])
		if !dirty || err != nil {
			return false, n, err
		}
		n = n.copy()
		n.flags = t.newFlag()
		n.Children[key[0]] = nn

		// Because n is a full node, it must've contained at least two children
		// before the delete operation. If the new child value is non-nil, n still
		// has at least two children after the deletion, and cannot be reduced to
		// a short node.
		if nn != nil {
			return true, n, nil
		}
		// Reduction:
		// Check how many non-nil entries are left after deleting and
		// reduce the full node to a short node if only one entry is
		// left. Since n must've contained at least two children
		// before deletion (otherwise it would not be a full node) n
		// can never be reduced to nil.
		//
		// When the loop is done, pos contains the index of the single
		// value that is left in n or -2 if n contains at least two
		// values.
		pos := -1
		for i, cld := range &n.Children {
			if cld != nil {
				if pos == -1 {
					pos = i
				} else {
					pos = -2
					break
				}
			}
		}
		if pos >= 0 {
			if pos != 16 {
				// If the remaining entry is a short node, it replaces
				// n and its key gets the missing nibble tacked to the
				// front. This avoids creating an invalid
				// shortNode{..., shortNode{...}}.  Since the entry
				// might not be loaded yet, resolve it just for this
				// check.
				cnode, err := t.resolve(n.Children[pos], append(prefix, byte(pos)))
				if err != nil {
					return false, nil, err
				}
				if cnode, ok := cnode.(*shortNode); ok {
					// Replace the entire full node with the short node.
					// Mark the original short node as deleted since the
					// value is embedded into the parent now.
					t.tracer.onDelete(append(prefix, byte(pos)))

					k := append([]byte{byte(pos)}, cnode.Key...)
					return true, &shortNode{k, cnode.Val, t.newFlag()}, nil
				}
			}
			// Otherwise, n is replaced by a one-nibble short node
			// containing the child.
			return true, &shortNode{[]byte{byte(pos)}, n.Children[pos], t.newFlag()}, nil
		}
		// n still contains at least two values and cannot be reduced.
		return true, n, nil

	case valueNode:
		return true, nil, nil

	case nil:
		return false, nil, nil

	case hashNode:
		// We've hit a part of the trie that isn't loaded yet. Load
		// the node and delete from it. This leaves all child nodes on
		// the path to the value in the trie.
		rn, err := t.resolveAndTrack(n, prefix)
		if err != nil {
			return false, nil, err
		}
		dirty, nn, err := t.delete(rn, prefix, key)
		if !dirty || err != nil {
			return false, rn, err
		}
		return true, nn, nil

	default:
		panic(fmt.Sprintf("%T: invalid node: %v (%v)", n, n, key))
	}
}

func concat(s1 []byte, s2 ...byte) []byte {
	r := make([]byte, len(s1)+len(s2))
	copy(r, s1)
	copy(r[len(s1):], s2)
	return r
}

func (t *Trie) resolve(n node, prefix []byte) (node, error) {
	if n, ok := n.(hashNode); ok {
		return t.resolveAndTrack(n, prefix)
	}
	return n, nil
}

/*
根据指定的 node hash 和 path prefix 从底层存储 loads node.
*/

// resolveAndTrack loads node from the underlying store with the given node hash
// and path prefix and also tracks the loaded node blob in tracer treated as the
// node's original value. The rlp-encoded blob is preferred to be loaded from
// database because it's easy to decode node while complex to encode node to blob.
func (t *Trie) resolveAndTrack(n hashNode, prefix []byte) (node, error) {
	blob, err := t.reader.node(prefix, common.BytesToHash(n))
	if err != nil {
		return nil, err
	}
	t.tracer.onRead(prefix, blob)
	return mustDecodeNode(n, blob), nil
}

// Hash returns the root hash of the trie. It does not write to the
// database and can be used even if the trie doesn't have one.
/*

 */
func (t *Trie) Hash() common.Hash {
	hash, cached := t.hashRoot()
	t.root = cached
	return common.BytesToHash(hash.(hashNode))
}

// Commit collects all dirty nodes in the trie and replaces them with the
// corresponding node hash. All collected nodes (including dirty leaves if
// collectLeaf is true) will be encapsulated into a nodeset for return.
// The returned nodeset can be nil if the trie is clean (nothing to commit).
// Once the trie is committed, it's not usable anymore. A new trie must
// be created with new root and updated trie database for following usage
/*
Commit函数提供将内存中的MPT数据持久化到数据库的功能. MPT具有快速计算所维护数据集哈希标识以快速状态回滚的能力，也都是在该函数中实现的。
在commit完成后，所有变脏的树节点会重新进行哈希计算，并且将新内容写入数据库；最终新的根节点哈希将被作为MPT的最新状态被返回。

一次MPT树提交是一个递归调用的过程，在介绍MPT提交过程之前，我们首先介绍单个节点是如何进行哈希计算和存储的。
单节点

1. 首先是对该节点进行脏位的判断，若当前节点未被修改，则直接返回该节点的哈希值，调用结束
（此外，若当前节点既未被修改，同时存在于内存的时间又”过长“，则将以该节点为根节点的子树从内存中驱除）； --- 在哪 ??
2. 该节点为脏节点，对该节点进行哈希重计算。首先是对当前节点的孩子节点进行哈希计算，对孩子节点的哈希计算是利用递归地对节点进行处理完成。这一步骤的目的是将孩子节点的信息各自转换成一个哈希值进行表示；。
3. 对当前节点进行哈希计算。哈希计算利用sha256哈希算法对当前节点的RLP编码进行哈希计算；
	3.1 对于分支节点来说，该节点的RLP编码就是对其孩子列表的内容进行编码，且在第二步中，所有的孩子节点所有已经被转换成了一个哈希值；
	3.2 对于叶子／扩展节点来说，该节点的RLP编码就是对其Key，Value字段进行编码。同样在第二步中，若Value指代的是另外一个节点的引用，则已经被转换成了一个哈希值（在第二步中，Key已经被转换成了HP编码）；
4. 将当前节点的数据存入数据库，存储的格式为[节点哈希值，节点的RLP编码]。
	---- Commit 过程利用 (c *committer) store 存储到 NodeSet 中, 后面再利用 pbss 的 Update 进行持久化 ?? MergedNodeSet
5. 将自身的dirty标志置为false，并将计算所得的哈希值进行缓存；

---
Commit 搜集 trie 内所有的 dirty nodes, 并使用对应的 node hash 进行替换; 所有收集的 nodes(包含 dirty leaves) 封装到 nodeset, 并返回;
返回值:
1) NodeSet:
---
1) (t *Trie) Hash() : 计算所有 dirty nodes 的 hash;
*/
func (t *Trie) Commit(collectLeaf bool) (common.Hash, *trienode.NodeSet, error) {
	defer t.tracer.reset()
	defer func() {
		t.committed = true
	}()
	// Trie is empty and can be classified into two types of situations:
	// (a) The trie was empty and no update happens => return nil
	// (b) The trie was non-empty and all nodes are dropped => return
	//     the node set includes all deleted nodes
	/*
		t.root
	*/
	if t.root == nil {
		paths := t.tracer.deletedNodes()
		if len(paths) == 0 {
			return types.EmptyRootHash, nil, nil // case (a)
		}
		nodes := trienode.NewNodeSet(t.owner)
		for _, path := range paths {
			nodes.AddNode([]byte(path), trienode.NewDeleted())
		}
		return types.EmptyRootHash, nodes, nil // case (b)
	}
	// Derive the hash for all dirty nodes first. We hold the assumption
	// in the following procedure that all nodes are hashed.
	// 所有 dirty nodes 的 hash;
	rootHash := t.Hash()

	// Do a quick check if we really need to commit. This can happen e.g.
	// if we load a trie for reading storage values, but don't write to it.
	if hashedNode, dirty := t.root.cache(); !dirty {
		// Replace the root node with the origin hash in order to
		// ensure all resolved nodes are dropped after the commit.
		t.root = hashedNode
		return rootHash, nil, nil
	}
	// 初始化 NodeSet, nodes 记录 Commit 过程更新的 nodes
	nodes := trienode.NewNodeSet(t.owner)
	for _, path := range t.tracer.deletedNodes() {
		nodes.AddNode([]byte(path), trienode.NewDeleted())
	}
	t.root = newCommitter(nodes, t.tracer, collectLeaf).Commit(t.root)
	return rootHash, nodes, nil
}

// hashRoot calculates the root hash of the given trie
func (t *Trie) hashRoot() (node, node) {
	if t.root == nil {
		return hashNode(types.EmptyRootHash.Bytes()), nil
	}
	// If the number of changes is below 100, we let one thread handle it
	h := newHasher(t.unhashed >= 100)
	defer func() {
		returnHasherToPool(h)
		t.unhashed = 0
	}()
	hashed, cached := h.hash(t.root, true)
	return hashed, cached
}

// Reset drops the referenced root node and cleans all internal state.
func (t *Trie) Reset() {
	t.root = nil
	t.owner = common.Hash{}
	t.unhashed = 0
	t.tracer.reset()
	t.committed = false
}

func (t *Trie) Size() int {
	return estimateSize(t.root)
}

// Owner returns the associated trie owner.
func (t *Trie) Owner() common.Hash {
	return t.owner
}
