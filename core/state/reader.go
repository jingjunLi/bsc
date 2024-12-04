// Copyright 2024 The go-ethereum Authors
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

package state

import (
	"bytes"
	"errors"
	"maps"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state/snapshot"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/ethereum/go-ethereum/trie/utils"
	"github.com/ethereum/go-ethereum/triedb"
)

// Reader defines the interface for accessing accounts and storage slots
// associated with a specific state.
type Reader interface {
	// Account retrieves the account associated with a particular address.
	//
	// - Returns a nil account if it does not exist
	// - Returns an error only if an unexpected issue occurs
	// - The returned account is safe to modify after the call
	Account(addr common.Address) (*types.StateAccount, error)

	// Storage retrieves the storage slot associated with a particular account
	// address and slot key.
	//
	// - Returns an empty slot if it does not exist
	// - Returns an error only if an unexpected issue occurs
	// - The returned storage slot is safe to modify after the call
	Storage(addr common.Address, slot common.Hash) (common.Hash, error)

	// Copy returns a deep-copied state reader.
	Copy() Reader
}

// stateReader is a wrapper over the state snapshot and implements the Reader
// interface. It provides an efficient way to access flat state.
type stateReader struct {
	db   *CachingDB
	snap snapshot.Snapshot
	buff crypto.KeccakState
}

// newStateReader constructs a flat state reader with on the specified state root.
func newStateReader(root common.Hash, snaps *snapshot.Tree, db *CachingDB) (*stateReader, error) {
	snap := snaps.Snapshot(root)
	if snap == nil {
		return nil, errors.New("snapshot is not available")
	}
	return &stateReader{
		snap: snap,
		buff: crypto.NewKeccakState(),
		db:   db,
	}, nil
}

// Account implements Reader, retrieving the account specified by the address.
//
// An error will be returned if the associated snapshot is already stale or
// the requested account is not yet covered by the snapshot.
//
// The returned account might be nil if it's not existent.
func (r *stateReader) Account(addr common.Address) (*types.StateAccount, error) {
	var lookupData []byte
	var err error
	accountAddrHash := crypto.HashData(r.buff, addr.Bytes())
	lookupAccount := new(types.SlimAccount)

	log.Info("stateReader Account 11", "addr", addr, "hash", accountAddrHash)
	// var lookupDone bool
	{
		// fastpath
		root := r.snap.Root()
		targetLayer := r.db.snap.LookupAccount(accountAddrHash, root)
		//targetLayer := snapshot.GlobalLookup.LookupAccount()
		if targetLayer != nil {
			lookupData, err = targetLayer.AccountRLP(accountAddrHash)
			if err != nil {
				log.Info("GlobalLookup.lookupAccount err", "hash", accountAddrHash, "root", root, "err", err)
			}
			if len(lookupData) == 0 { // can be both nil and []byte{}
				log.Info("GlobalLookup.lookupAccount data nil", "hash", accountAddrHash, "root", root)
			}
			if err == nil && len(lookupData) != 0 {
				if err := rlp.DecodeBytes(lookupData, lookupAccount); err != nil {
					panic(err)
				}
				// lookupDone = true
			} else {
				log.Info("GlobalLookup.lookupAccount", "hash", accountAddrHash, "root", root, "res", lookupData, "targetLayer", targetLayer)
			}
			//log.Info("GlobalLookup.lookupAccount", "hash", accountAddrHash, "root", root, "res", lookupData, "targetLayer", targetLayer)
		}
	}
	ret, err := r.snap.Account(accountAddrHash)
	if err != nil {
		return nil, err
	}
	if ret == nil {
		return nil, nil
	}
	acct := &types.StateAccount{
		Nonce:    ret.Nonce,
		Balance:  ret.Balance,
		CodeHash: ret.CodeHash,
		Root:     common.BytesToHash(ret.Root),
	}
	if len(acct.CodeHash) == 0 {
		acct.CodeHash = types.EmptyCodeHash.Bytes()
	}
	if acct.Root == (common.Hash{}) {
		acct.Root = types.EmptyRootHash
	}

	if ret.Nonce != lookupAccount.Nonce ||
		!bytes.Equal(ret.Root, lookupAccount.Root) {
		log.Info("stateReader Account not same real account", "real data", ret, "lookupData", lookupAccount)
	}

	return acct, nil
}

// Storage implements Reader, retrieving the storage slot specified by the
// address and slot key.
//
// An error will be returned if the associated snapshot is already stale or
// the requested storage slot is not yet covered by the snapshot.
//
// The returned storage slot might be empty if it's not existent.
func (r *stateReader) Storage(addr common.Address, key common.Hash) (common.Hash, error) {
	addrHash := crypto.HashData(r.buff, addr.Bytes())
	slotHash := crypto.HashData(r.buff, key.Bytes())

	var lookupData []byte
	var err error
	// var lookupDone bool
	// log.Info("stateReader Storage 11", "addr", addr, "key", key, "addrHash", addrHash, "slotHash", slotHash)
	{
		// fastpath
		//targetLayer := snapshot.GlobalLookup.LookupStorage(addrHash, slotHash, r.snap.Root())
		targetLayer := r.db.snap.LookupStorage(addrHash, slotHash, r.snap.Root())
		if targetLayer != nil {
			lookupData, err = targetLayer.Storage(addrHash, slotHash)
			if err != nil {
				log.Info("GlobalLookup.lookupStorage err", "addrHash", addrHash, "slotHash", slotHash, "err", err)
			}
			if len(lookupData) == 0 { // can be both nil and []byte{}
				log.Info("GlobalLookup.lookupStorage data nil", "addrHash", addrHash, "slotHash", slotHash)
			}
			if err == nil && len(lookupData) != 0 {
			} else {
				log.Info("GlobalLookup.lookupStorage", "addrHash", addrHash, "slotHash", slotHash, "res", lookupData)
			}
			//return targetLayer.Storage(accountHash, storageHash)
		}
		// log.Info("GlobalLookup.lookupStorage", "addrHash", addrHash, "slotHash", slotHash, "res", lookupData)
	}

	ret, err := r.snap.Storage(addrHash, slotHash)
	if err != nil {
		return common.Hash{}, err
	}
	if len(ret) == 0 {
		return common.Hash{}, nil
	}

	if !bytes.Equal(ret, lookupData) {
		log.Info("stateReader Storage not same real storage", "data", ret, "lookupData", lookupData)
	}
	// Perform the rlp-decode as the slot value is RLP-encoded in the state
	// snapshot.
	_, content, _, err := rlp.Split(ret)
	if err != nil {
		return common.Hash{}, err
	}
	var value common.Hash
	value.SetBytes(content)
	return value, nil
}

// Copy implements Reader, returning a deep-copied snap reader.
func (r *stateReader) Copy() Reader {
	return &stateReader{
		snap: r.snap,
		buff: crypto.NewKeccakState(),
	}
}

// trieReader implements the Reader interface, providing functions to access
// state from the referenced trie.
type trieReader struct {
	root     common.Hash                    // State root which uniquely represent a state
	db       *triedb.Database               // Database for loading trie
	buff     crypto.KeccakState             // Buffer for keccak256 hashing
	mainTrie Trie                           // Main trie, resolved in constructor
	subRoots map[common.Address]common.Hash // Set of storage roots, cached when the account is resolved
	subTries map[common.Address]Trie        // Group of storage tries, cached when it's resolved
}

// trieReader constructs a trie reader of the specific state. An error will be
// returned if the associated trie specified by root is not existent.
func newTrieReader(root common.Hash, db *triedb.Database, cache *utils.PointCache) (*trieReader, error) {
	var (
		tr  Trie
		err error
	)
	if !db.IsVerkle() {
		tr, err = trie.NewStateTrie(trie.StateTrieID(root), db)
	} else {
		tr, err = trie.NewVerkleTrie(root, db, cache)
	}
	if err != nil {
		return nil, err
	}
	return &trieReader{
		root:     root,
		db:       db,
		buff:     crypto.NewKeccakState(),
		mainTrie: tr,
		subRoots: make(map[common.Address]common.Hash),
		subTries: make(map[common.Address]Trie),
	}, nil
}

// Account implements Reader, retrieving the account specified by the address.
//
// An error will be returned if the trie state is corrupted. An nil account
// will be returned if it's not existent in the trie.
func (r *trieReader) Account(addr common.Address) (*types.StateAccount, error) {
	account, err := r.mainTrie.GetAccount(addr)
	if err != nil {
		return nil, err
	}
	if account == nil {
		r.subRoots[addr] = types.EmptyRootHash
	} else {
		r.subRoots[addr] = account.Root
	}
	return account, nil
}

// Storage implements Reader, retrieving the storage slot specified by the
// address and slot key.
//
// An error will be returned if the trie state is corrupted. An empty storage
// slot will be returned if it's not existent in the trie.
func (r *trieReader) Storage(addr common.Address, key common.Hash) (common.Hash, error) {
	var (
		tr    Trie
		found bool
		value common.Hash
	)
	if r.db.IsVerkle() {
		tr = r.mainTrie
	} else {
		tr, found = r.subTries[addr]
		if !found {
			root, ok := r.subRoots[addr]

			// The storage slot is accessed without account caching. It's unexpected
			// behavior but try to resolve the account first anyway.
			if !ok {
				_, err := r.Account(addr)
				if err != nil {
					return common.Hash{}, err
				}
				root = r.subRoots[addr]
			}
			var err error
			tr, err = trie.NewStateTrie(trie.StorageTrieID(r.root, crypto.HashData(r.buff, addr.Bytes()), root), r.db)
			if err != nil {
				return common.Hash{}, err
			}
			r.subTries[addr] = tr
		}
	}
	ret, err := tr.GetStorage(addr, key.Bytes())
	if err != nil {
		return common.Hash{}, err
	}
	value.SetBytes(ret)
	return value, nil
}

// Copy implements Reader, returning a deep-copied trie reader.
func (r *trieReader) Copy() Reader {
	tries := make(map[common.Address]Trie)
	for addr, tr := range r.subTries {
		tries[addr] = mustCopyTrie(tr)
	}
	return &trieReader{
		root:     r.root,
		db:       r.db,
		buff:     crypto.NewKeccakState(),
		mainTrie: mustCopyTrie(r.mainTrie),
		subRoots: maps.Clone(r.subRoots),
		subTries: tries,
	}
}

// multiReader is the aggregation of a list of Reader interface, providing state
// access by leveraging all readers. The checking priority is determined by the
// position in the reader list.
type multiReader struct {
	readers []Reader // List of readers, sorted by checking priority
}

// newMultiReader constructs a multiReader instance with the given readers. The
// priority among readers is assumed to be sorted. Note, it must contain at least
// one reader for constructing a multiReader.
func newMultiReader(readers ...Reader) (*multiReader, error) {
	if len(readers) == 0 {
		return nil, errors.New("empty reader set")
	}
	return &multiReader{
		readers: readers,
	}, nil
}

// Account implementing Reader interface, retrieving the account associated with
// a particular address.
//
// - Returns a nil account if it does not exist
// - Returns an error only if an unexpected issue occurs
// - The returned account is safe to modify after the call
func (r *multiReader) Account(addr common.Address) (*types.StateAccount, error) {
	var errs []error
	for _, reader := range r.readers {
		acct, err := reader.Account(addr)
		if err == nil {
			return acct, nil
		}
		errs = append(errs, err)
	}
	return nil, errors.Join(errs...)
}

// Storage implementing Reader interface, retrieving the storage slot associated
// with a particular account address and slot key.
//
// - Returns an empty slot if it does not exist
// - Returns an error only if an unexpected issue occurs
// - The returned storage slot is safe to modify after the call
func (r *multiReader) Storage(addr common.Address, slot common.Hash) (common.Hash, error) {
	var errs []error
	for _, reader := range r.readers {
		slot, err := reader.Storage(addr, slot)
		if err == nil {
			return slot, nil
		}
		errs = append(errs, err)
	}
	return common.Hash{}, errors.Join(errs...)
}

// Copy implementing Reader interface, returning a deep-copied state reader.
func (r *multiReader) Copy() Reader {
	var readers []Reader
	for _, reader := range r.readers {
		readers = append(readers, reader.Copy())
	}
	return &multiReader{readers: readers}
}
