// Copyright 2020 The go-ethereum Authors
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

package trie

import (
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie/trienode"
)

// committer is the tool used for the trie Commit operation. The committer will
// capture all dirty nodes during the commit process and keep them cached in
// insertion order.
/*
committer 是 trie Commit 操作的工具; 在 commit 过程中, committer 会捕获所有 dirty nodes ; 并且按照插入顺序保持它们的缓存;
committer 是
核心的函数:
1) Commit -> commit
2)
*/
type committer struct {
	nodes       *trienode.NodeSet
	tracer      *tracer
	collectLeaf bool
}

// newCommitter creates a new committer or picks one from the pool.
func newCommitter(nodeset *trienode.NodeSet, tracer *tracer, collectLeaf bool) *committer {
	return &committer{
		nodes:       nodeset,
		tracer:      tracer,
		collectLeaf: collectLeaf,
	}
}

// Commit collapses a node down into a hash node.
// 调用方 : trie.Commit 什么用途 ?
func (c *committer) Commit(n node) hashNode {
	return c.commit(nil, n).(hashNode)
}

// commit collapses a node down into a hash node and returns it.
/*
commit 将节点折叠为哈希节点并返回它。 这里是一个递归的调用过程.
collapsed 是 mpt 树提交的过程, value 是存储的子节点的 hash.

1) 将 node 进行 copy , 命名为 collapsed
2) collapsed 节点 对应的 字段 存储的都是 子节点的 hash
2.1) shortNode

(c *committer) store
*/
func (c *committer) commit(path []byte, n node) node {
	// if this path is clean, use available cached data
	// 1. 首先是对该节点进行脏位的判断，若当前节点未被修改，则直接返回该节点的哈希值，调用结束
	hash, dirty := n.cache()
	if hash != nil && !dirty {
		return hash
	}
	// Commit children, then parent, and remove the dirty flag.
	/*
		2. 该节点为脏节点，对该节点进行哈希重计算。首先是对当前节点的孩子节点进行哈希计算，对孩子节点的哈希计算是利用递归地对节点进行处理完成。
		这一步骤的目的是将孩子节点的信息各自转换成一个哈希值进行表示；。
		3. 对当前节点进行哈希计算。哈希计算利用sha256哈希算法对当前节点的RLP编码进行哈希计算；
	*/
	switch cn := n.(type) {
	case *shortNode:
		/*
			3.2 对于叶子／扩展节点来说，该节点的RLP编码就是对其Key，Value字段进行编码。
			同样在第二步中，若Value指代的是另外一个节点的引用，则已经被转换成了一个哈希值（在第二步中，Key已经被转换成了HP编码）；
		*/
		// Commit child
		collapsed := cn.copy()

		// If the child is fullNode, recursively commit,
		// otherwise it can only be hashNode or valueNode.
		if _, ok := cn.Val.(*fullNode); ok {
			collapsed.Val = c.commit(append(path, cn.Key...), cn.Val)
		}
		// The key needs to be copied, since we're adding it to the
		// modified nodeset.
		collapsed.Key = hexToCompact(cn.Key)
		hashedNode := c.store(path, collapsed)
		if hn, ok := hashedNode.(hashNode); ok {
			return hn
		}
		return collapsed
	case *fullNode:
		/*
			3.1 对于分支节点来说，该节点的RLP编码就是对其孩子列表的内容进行编码，且在第二步中，所有的孩子节点所有已经被转换成了一个哈希值；
		*/
		hashedKids := c.commitChildren(path, cn)
		collapsed := cn.copy()
		collapsed.Children = hashedKids

		hashedNode := c.store(path, collapsed)
		if hn, ok := hashedNode.(hashNode); ok {
			return hn
		}
		return collapsed
	case hashNode:
		return cn
	default:
		// nil, valuenode shouldn't be committed
		panic(fmt.Sprintf("%T: invalid node: %v", n, n))
	}
}

// commitChildren commits the children of the given fullnode
func (c *committer) commitChildren(path []byte, n *fullNode) [17]node {
	var children [17]node
	for i := 0; i < 16; i++ {
		child := n.Children[i]
		if child == nil {
			continue
		}
		// If it's the hashed child, save the hash value directly.
		// Note: it's impossible that the child in range [0, 15]
		// is a valueNode.
		if hn, ok := child.(hashNode); ok {
			children[i] = hn
			continue
		}
		// Commit the child recursively and store the "hashed" value.
		// Note the returned node can be some embedded nodes, so it's
		// possible the type is not hashNode.
		/*
			1) embedded nodes
		*/
		children[i] = c.commit(append(path, byte(i)), child)
	}
	// For the 17th child, it's possible the type is valuenode.
	if n.Children[16] != nil {
		children[16] = n.Children[16]
	}
	return children
}

// store hashes the node n and adds it to the modified nodeset. If leaf collection
// is enabled, leaf nodes will be tracked in the modified nodeset as well.
/*
对 node n 做 hash, 然后将其添加到 nodeset;
将当前节点的数据存入数据库，存储的格式为[节点哈希值，节点的RLP编码](hash mode 保存方式, pbss 已经优化 ???)。 ???
*/
func (c *committer) store(path []byte, n node) node {
	// Larger nodes are replaced by their hash and stored in the database.
	var hash, _ = n.cache()

	// This was not generated - must be a small node stored in the parent.
	// In theory, we should check if the node is leaf here (embedded node
	// usually is leaf node). But small value (less than 32bytes) is not
	// our target (leaves in account trie only).
	/*
	 */
	if hash == nil {
		// The node is embedded in its parent, in other words, this node
		// will not be stored in the database independently, mark it as
		// deleted only if the node was existent in database before.
		_, ok := c.tracer.accessList[string(path)]
		if ok {
			c.nodes.AddNode(path, trienode.NewDeleted())
		}
		return n
	}
	// Collect the dirty node to nodeset for return.
	nhash := common.BytesToHash(hash)
	c.nodes.AddNode(path, trienode.New(nhash, nodeToBytes(n)))

	// Collect the corresponding leaf node if it's required. We don't check
	// full node since it's impossible to store value in fullNode. The key
	// length of leaves should be exactly same.
	if c.collectLeaf {
		if sn, ok := n.(*shortNode); ok {
			if val, ok := sn.Val.(valueNode); ok {
				c.nodes.AddLeaf(nhash, val)
			}
		}
	}
	return hash
}

// estimateSize estimates the size of an rlp-encoded node, without actually
// rlp-encoding it (zero allocs). This method has been experimentally tried, and with a trie
// with 1000 leafs, the only errors above 1% are on small shortnodes, where this
// method overestimates by 2 or 3 bytes (e.g. 37 instead of 35)
func estimateSize(n node) int {
	switch n := n.(type) {
	case *shortNode:
		// A short node contains a compacted key, and a value.
		return 3 + len(n.Key) + estimateSize(n.Val)
	case *fullNode:
		// A full node contains up to 16 hashes (some nils), and a key
		s := 3
		for i := 0; i < 16; i++ {
			if child := n.Children[i]; child != nil {
				s += estimateSize(child)
			} else {
				s++
			}
		}
		return s
	case valueNode:
		return 1 + len(n)
	case hashNode:
		return 1 + len(n)
	default:
		panic(fmt.Sprintf("node type %T", n))
	}
}

// MerkleResolver the children resolver in merkle-patricia-tree.
type MerkleResolver struct{}

// ForEach implements childResolver, decodes the provided node and
// traverses the children inside.
func (resolver MerkleResolver) ForEach(node []byte, onChild func(common.Hash)) {
	forGatherChildren(mustDecodeNodeUnsafe(nil, node), onChild)
}

// forGatherChildren traverses the node hierarchy and invokes the callback
// for all the hashnode children.
func forGatherChildren(n node, onChild func(hash common.Hash)) {
	switch n := n.(type) {
	case *shortNode:
		forGatherChildren(n.Val, onChild)
	case *fullNode:
		for i := 0; i < 16; i++ {
			forGatherChildren(n.Children[i], onChild)
		}
	case hashNode:
		onChild(common.BytesToHash(n))
	case valueNode, nil:
	default:
		panic(fmt.Sprintf("unknown node type: %T", n))
	}
}
