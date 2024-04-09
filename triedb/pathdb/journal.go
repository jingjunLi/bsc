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
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/trie/trienode"
	"github.com/ethereum/go-ethereum/trie/triestate"
	"github.com/go-git/go-git/v5/utils/ioutil"
)

var (
	errMissJournal       = errors.New("journal not found")
	errMissVersion       = errors.New("version not found")
	errUnexpectedVersion = errors.New("unexpected journal version")
	errMissDiskRoot      = errors.New("disk layer root not found")
	errUnmatchedJournal  = errors.New("unmatched journal")
)

const journalVersion uint64 = 0

// journalNode represents a trie node persisted in the journal.
type journalNode struct {
	Path []byte // Path of the node in the trie
	Blob []byte // RLP-encoded trie node blob, nil means the node is deleted
}

// journalNodes represents a list trie nodes belong to a single account
// or the main account trie.
type journalNodes struct {
	Owner common.Hash
	Nodes []journalNode
}

// journalAccounts represents a list accounts belong to the layer.
type journalAccounts struct {
	Addresses []common.Address
	Accounts  [][]byte
}

// journalStorage represents a list of storage slots belong to an account.
type journalStorage struct {
	Incomplete bool
	Account    common.Address
	Hashes     []common.Hash
	Slots      [][]byte
}

// journalKV is used to store the journal as a single KV in database.
type journalKV struct {
	Journal
	journalBuf bytes.Buffer
	diskdb     ethdb.Database
}

// journalFile is used to store trie journal into a file.
type journalFile struct {
	Journal
	file string // the file used to store the TrieJournal
}

// loadJournal tries to parse the layer journal from the disk.
func (db *Database) loadJournal(diskRoot common.Hash) (layer, error) {
	start := time.Now()
	reader, err := db.journal.newJournalReader()

	if err != nil {
		return nil, err
	}
	if reader != nil {
		defer reader.Close()
	}
	r := rlp.NewStream(reader, 0)

	// Firstly, resolve the first element as the journal version
	version, err := r.Uint64()
	if err != nil {
		return nil, errMissVersion
	}
	if version != journalVersion {
		return nil, fmt.Errorf("%w want %d got %d", errUnexpectedVersion, journalVersion, version)
	}
	// Secondly, resolve the disk layer root, ensure it's continuous
	// with disk layer. Note now we can ensure it's the layer journal
	// correct version, so we expect everything can be resolved properly.
	var root common.Hash
	if err := r.Decode(&root); err != nil {
		return nil, errMissDiskRoot
	}
	// The journal is not matched with persistent state, discard them.
	// It can happen that geth crashes without persisting the journal.
	if !bytes.Equal(root.Bytes(), diskRoot.Bytes()) {
		return nil, fmt.Errorf("%w want %x got %x", errUnmatchedJournal, root, diskRoot)
	}
	// Load the disk layer from the journal
	base, err := db.loadDiskLayer(r)
	if err != nil {
		return nil, err
	}
	// Load all the diff layers from the journal
	head, err := db.loadDiffLayer(base, r)
	if err != nil {
		return nil, err
	}
	log.Info("Loaded layer journal", "diskroot", diskRoot, "diffhead", head.rootHash(), "elapsed", common.PrettyDuration(time.Since(start)))
	return head, nil
}

// loadLayers loads a pre-existing state layer backed by a key-value store.
func (db *Database) loadLayers() layer {
	// Retrieve the root node of persistent state.
	_, root := rawdb.ReadAccountTrieNode(db.diskdb, nil)
	root = types.TrieRootHash(root)

	// Load the layers by resolving the journal
	head, err := db.loadJournal(root)
	if err == nil {
		return head
	}
	// journal is not matched(or missing) with the persistent state, discard
	// it. Display log for discarding journal, but try to avoid showing
	// useless information when the db is created from scratch.
	if !(root == types.EmptyRootHash && errors.Is(err, errMissJournal)) {
		log.Info("Failed to load journal, discard it", "err", err)
	}
	// Return single layer with persistent state.
	return newDiskLayer(root, rawdb.ReadPersistentStateID(db.diskdb), db, nil, NewTrieNodeBuffer(db.config.SyncFlush, db.bufferSize, nil, 0))
}

// loadDiskLayer reads the binary blob from the layer journal, reconstructing
// a new disk layer on it.
func (db *Database) loadDiskLayer(r *rlp.Stream) (layer, error) {
	// Resolve disk layer root
	var root common.Hash
	var length uint64
	if err := r.Decode(&length); err != nil {
		return nil, fmt.Errorf("load disk length: %v", err)
	}

	var journalEncodedBuff []byte
	if err := r.Decode(&journalEncodedBuff); err != nil {
		return nil, fmt.Errorf("load disk journal: %v", err)
	}

	journalBuf := rlp.NewStream(bytes.NewReader(journalEncodedBuff), 0)
	if err := journalBuf.Decode(&root); err != nil {
		return nil, fmt.Errorf("load disk root: %v", err)
	}
	// Resolve the state id of disk layer, it can be different
	// with the persistent id tracked in disk, the id distance
	// is the number of transitions aggregated in disk layer.
	var id uint64
	if err := journalBuf.Decode(&id); err != nil {
		return nil, fmt.Errorf("load state id: %v", err)
	}
	stored := rawdb.ReadPersistentStateID(db.diskdb)
	if stored > id {
		return nil, fmt.Errorf("invalid state id: stored %d resolved %d", stored, id)
	}
	// Resolve nodes cached in node buffer
	var encoded []journalNodes
	if err := journalBuf.Decode(&encoded); err != nil {
		return nil, fmt.Errorf("load disk nodes: %v", err)
	}
	nodes := make(map[common.Hash]map[string]*trienode.Node)
	for _, entry := range encoded {
		subset := make(map[string]*trienode.Node)
		for _, n := range entry.Nodes {
			if len(n.Blob) > 0 {
				subset[string(n.Path)] = trienode.New(crypto.Keccak256Hash(n.Blob), n.Blob)
			} else {
				subset[string(n.Path)] = trienode.NewDeleted()
			}
		}
		nodes[entry.Owner] = subset
	}

	var shaSum [32]byte
	if err := r.Decode(&shaSum); err != nil {
		return nil, fmt.Errorf("load shasum: %v", err)
	}

	expectSum := sha256.Sum256(journalEncodedBuff)
	if shaSum != expectSum {
		return nil, fmt.Errorf("expect shaSum: %v, real:%v", expectSum, shaSum)
	}

	// Calculate the internal state transitions by id difference.
	base := newDiskLayer(root, id, db, nil, NewTrieNodeBuffer(db.config.SyncFlush, db.bufferSize, nodes, id-stored))
	return base, nil
}

// loadDiffLayer reads the next sections of a layer journal, reconstructing a new
// diff and verifying that it can be linked to the requested parent.
func (db *Database) loadDiffLayer(parent layer, r *rlp.Stream) (layer, error) {
	// Read the next diff journal entry
	var root common.Hash
	var length uint64
	if err := r.Decode(&length); err != nil {
		// The first read may fail with EOF, marking the end of the journal
		if err == io.EOF {
			return parent, nil
		}
		return nil, fmt.Errorf("load disk length : %v", err)
	}
	var journalEncodedBuff []byte
	if err := r.Decode(&journalEncodedBuff); err != nil {
		return nil, fmt.Errorf("load disk journal buffer: %v", err)
	}

	journalBuf := rlp.NewStream(bytes.NewReader(journalEncodedBuff), 0)

	if err := journalBuf.Decode(&root); err != nil {
		// The first read may fail with EOF, marking the end of the journal
		if err == io.EOF {
			return parent, nil
		}
		return nil, fmt.Errorf("load diff root: %v", err)
	}
	var block uint64
	if err := journalBuf.Decode(&block); err != nil {
		return nil, fmt.Errorf("load block number: %v", err)
	}
	// Read in-memory trie nodes from journal
	var encoded []journalNodes
	if err := journalBuf.Decode(&encoded); err != nil {
		return nil, fmt.Errorf("load diff nodes: %v", err)
	}
	nodes := make(map[common.Hash]map[string]*trienode.Node)
	for _, entry := range encoded {
		subset := make(map[string]*trienode.Node)
		for _, n := range entry.Nodes {
			if len(n.Blob) > 0 {
				subset[string(n.Path)] = trienode.New(crypto.Keccak256Hash(n.Blob), n.Blob)
			} else {
				subset[string(n.Path)] = trienode.NewDeleted()
			}
		}
		nodes[entry.Owner] = subset
	}
	// Read state changes from journal
	var (
		jaccounts  journalAccounts
		jstorages  []journalStorage
		accounts   = make(map[common.Address][]byte)
		storages   = make(map[common.Address]map[common.Hash][]byte)
		incomplete = make(map[common.Address]struct{})
	)
	if err := journalBuf.Decode(&jaccounts); err != nil {
		return nil, fmt.Errorf("load diff accounts: %v", err)
	}
	for i, addr := range jaccounts.Addresses {
		accounts[addr] = jaccounts.Accounts[i]
	}
	if err := journalBuf.Decode(&jstorages); err != nil {
		return nil, fmt.Errorf("load diff storages: %v", err)
	}
	for _, entry := range jstorages {
		set := make(map[common.Hash][]byte)
		for i, h := range entry.Hashes {
			if len(entry.Slots[i]) > 0 {
				set[h] = entry.Slots[i]
			} else {
				set[h] = nil
			}
		}
		if entry.Incomplete {
			incomplete[entry.Account] = struct{}{}
		}
		storages[entry.Account] = set
	}
	var shaSum [32]byte
	if err := r.Decode(&shaSum); err != nil {
		return nil, fmt.Errorf("load shasum: %v", err)
	}

	expectSum := sha256.Sum256(journalEncodedBuff)
	if shaSum != expectSum {
		return nil, fmt.Errorf("expect shaSum: %v, real:%v", expectSum, shaSum)
	}
	log.Info("Loaded diff layer journal", "root", root, "parent", parent.rootHash(), "id", parent.stateID()+1, "block", block)

	return db.loadDiffLayer(newDiffLayer(parent, root, parent.stateID()+1, block, nodes, triestate.New(accounts, storages, incomplete)), r)
}

// journal implements the layer interface, marshaling the un-flushed trie nodes
// along with layer metadata into provided byte buffer.
func (dl *diskLayer) journal(w io.Writer) error {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	// Create a buffer to store encoded data
	journalBuf := new(bytes.Buffer)

	// Ensure the layer didn't get stale
	if dl.stale {
		return errSnapshotStale
	}
	// Step one, write the disk root into the journal.
	if err := rlp.Encode(journalBuf, dl.root); err != nil {
		return err
	}
	// Step two, write the corresponding state id into the journal
	if err := rlp.Encode(journalBuf, dl.id); err != nil {
		return err
	}
	// Step three, write all unwritten nodes into the journal
	bufferNodes := dl.buffer.getAllNodes()
	nodes := make([]journalNodes, 0, len(bufferNodes))
	for owner, subset := range bufferNodes {
		entry := journalNodes{Owner: owner}
		for path, node := range subset {
			entry.Nodes = append(entry.Nodes, journalNode{Path: []byte(path), Blob: node.Blob})
		}
		nodes = append(nodes, entry)
	}
	if err := rlp.Encode(journalBuf, nodes); err != nil {
		return err
	}

	// Store the journal buf into w and calculate checksum
	if err := rlp.Encode(w, uint64(journalBuf.Len())); err != nil {
		return err
	}
	shasum := sha256.Sum256(journalBuf.Bytes())
	if err := rlp.Encode(w, journalBuf.Bytes()); err != nil {
		return err
	}
	if err := rlp.Encode(w, shasum); err != nil {
		return err
	}
	log.Info("Journaled pathdb disk layer", "root", dl.root, "nodes", len(bufferNodes))
	return nil
}

// journal implements the layer interface, writing the memory layer contents
// into a buffer to be stored in the database as the layer journal.
func (dl *diffLayer) journal(w io.Writer) error {
	dl.lock.RLock()
	defer dl.lock.RUnlock()

	// journal the parent first
	if err := dl.parent.journal(w); err != nil {
		return err
	}
	// Create a buffer to store encoded data
	journalBuf := new(bytes.Buffer)
	// Everything below was journaled, persist this layer too
	if err := rlp.Encode(journalBuf, dl.root); err != nil {
		return err
	}
	if err := rlp.Encode(journalBuf, dl.block); err != nil {
		return err
	}
	// Write the accumulated trie nodes into buffer
	nodes := make([]journalNodes, 0, len(dl.nodes))
	for owner, subset := range dl.nodes {
		entry := journalNodes{Owner: owner}
		for path, node := range subset {
			entry.Nodes = append(entry.Nodes, journalNode{Path: []byte(path), Blob: node.Blob})
		}
		nodes = append(nodes, entry)
	}
	if err := rlp.Encode(journalBuf, nodes); err != nil {
		return err
	}
	// Write the accumulated state changes into buffer
	var jacct journalAccounts
	for addr, account := range dl.states.Accounts {
		jacct.Addresses = append(jacct.Addresses, addr)
		jacct.Accounts = append(jacct.Accounts, account)
	}
	if err := rlp.Encode(journalBuf, jacct); err != nil {
		return err
	}
	storage := make([]journalStorage, 0, len(dl.states.Storages))
	for addr, slots := range dl.states.Storages {
		entry := journalStorage{Account: addr}
		if _, ok := dl.states.Incomplete[addr]; ok {
			entry.Incomplete = true
		}
		for slotHash, slot := range slots {
			entry.Hashes = append(entry.Hashes, slotHash)
			entry.Slots = append(entry.Slots, slot)
		}
		storage = append(storage, entry)
	}
	if err := rlp.Encode(journalBuf, storage); err != nil {
		return err
	}

	// Store the journal buf into w and calculate checksum
	if err := rlp.Encode(w, uint64(journalBuf.Len())); err != nil {
		return err
	}
	shasum := sha256.Sum256(journalBuf.Bytes())
	if err := rlp.Encode(w, journalBuf.Bytes()); err != nil {
		return err
	}
	if err := rlp.Encode(w, shasum); err != nil {
		return err
	}
	log.Info("Journaled pathdb diff layer", "root", dl.root, "parent", dl.parent.rootHash(), "id", dl.stateID(), "block", dl.block, "nodes", len(dl.nodes))
	return nil
}

// Journal commits an entire diff hierarchy to disk into a single journal entry.
// This is meant to be used during shutdown to persist the layer without
// flattening everything down (bad for reorgs). And this function will mark the
// database as read-only to prevent all following mutation to disk.
func (db *Database) Journal(root common.Hash) error {
	// Run the journaling
	db.lock.Lock()
	defer db.lock.Unlock()

	// Retrieve the head layer to journal from.
	l := db.tree.get(root)
	if l == nil {
		return fmt.Errorf("triedb layer [%#x] missing", root)
	}
	disk := db.tree.bottom()
	if l, ok := l.(*diffLayer); ok {
		log.Info("Persisting dirty state to disk", "head", l.block, "root", root, "layers", l.id-disk.id+disk.buffer.getLayers())
	} else { // disk layer only on noop runs (likely) or deep reorgs (unlikely)
		log.Info("Persisting dirty state to disk", "root", root, "layers", disk.buffer.getLayers())
	}
	start := time.Now()

	// wait and stop the flush trienodebuffer, for asyncnodebuffer need fixed diskroot
	disk.buffer.waitAndStopFlushing()
	// Short circuit if the database is in read only mode.
	if db.readOnly {
		return errDatabaseReadOnly
	}
	// Firstly write out the metadata of journal
	db.journal.Delete()
	journal := db.journal.newJournalWriter()
	defer journal.Close()

	if err := rlp.Encode(journal, journalVersion); err != nil {
		return err
	}
	// The stored state in disk might be empty, convert the
	// root to emptyRoot in this case.
	_, diskroot := rawdb.ReadAccountTrieNode(db.diskdb, nil)
	diskroot = types.TrieRootHash(diskroot)

	// Secondly write out the state root in disk, ensure all layers
	// on top are continuous with disk.
	if err := rlp.Encode(journal, diskroot); err != nil {
		return err
	}
	// Finally write out the journal of each layer in reverse order.
	if err := l.journal(journal); err != nil {
		return err
	}

	// Store the journal into the database and return
	// Size returns the size of the journal in bytes, It must be called before Sync.
	journalSize := db.journal.Size()
	db.journal.Sync()

	// Set the db in read only mode to reject all following mutations
	db.readOnly = true
	log.Info("Persisted dirty state to disk", "size", common.StorageSize(journalSize), "elapsed", common.PrettyDuration(time.Since(start)))
	return nil
}

type Journal interface {
	// newJournalWriter creates a new journal writer.
	newJournalWriter() io.WriteCloser

	// newJournalReader creates a new journal reader.
	newJournalReader() (io.ReadCloser, error)

	// Sync flushes the journal writer.
	Sync()

	// Delete deletes the journal.
	Delete()

	// Size returns the size of the journal.
	Size() uint64
}

func newJournal(file string, db ethdb.Database) Journal {
	if len(file) == 0 {
		return &journalKV{
			diskdb: db,
		}
	} else {
		return &journalFile{
			file: file,
		}
	}
}

func (kv *journalKV) newJournalWriter() io.WriteCloser {
	return ioutil.WriteNopCloser(&kv.journalBuf)
}

func (kv *journalKV) newJournalReader() (io.ReadCloser, error) {
	journal := rawdb.ReadTrieJournal(kv.diskdb)
	if len(journal) == 0 {
		return nil, errMissJournal
	}

	return io.NopCloser(bytes.NewBuffer(journal)), nil
}

func (kv *journalKV) Sync() {
	rawdb.WriteTrieJournal(kv.diskdb, kv.journalBuf.Bytes())
	kv.journalBuf.Reset()
}

func (kv *journalKV) Delete() {
	rawdb.DeleteTrieJournal(kv.diskdb)
}

func (kv *journalKV) Size() uint64 {
	return uint64(kv.journalBuf.Len())
}

// newJournalWriter creates a new journal writer.
func (f *journalFile) newJournalWriter() io.WriteCloser {
	fd, err := os.OpenFile(f.file, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil
	}
	return fd
}

// newJournalReader creates a new journal reader.
func (f *journalFile) newJournalReader() (io.ReadCloser, error) {
	fd, err := os.Open(f.file)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, errMissJournal
	}
	if err != nil {
		return nil, err
	}
	return fd, nil
}

// Sync flushes the journal writer.
func (f *journalFile) Sync() {
}

// Delete deletes the journal.
func (f *journalFile) Delete() {
	file := f.file
	_, err := os.Stat(file)
	if os.IsNotExist(err) {
		return
	}
	errRemove := os.Remove(file)
	if errRemove != nil {
		log.Crit("Failed to remove tries journal", "journal path", file, "err", err)
	}
}

// Size returns the size of the journal.
func (f *journalFile) Size() uint64 {
	fd, err := os.Open(f.file)
	if err != nil {
		return 0
	}
	defer fd.Close()
	fileInfo, err := fd.Stat()
	if err != nil {
		log.Crit("Failed to stat journal", "err", err)
	}
	return uint64(fileInfo.Size())
}
