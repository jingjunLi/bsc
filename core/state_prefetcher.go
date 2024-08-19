// Copyright 2019 The go-ethereum Authors
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

package core

import (
	"github.com/ethereum/go-ethereum/consensus"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/params"
)

const prefetchThread = 3
const checkInterval = 10

/*
statePrefetcher 是一个基本的 Prefetcher，它在任意状态之上盲目地执行区块，目的是在主区块处理器开始执行之前，从磁盘中预取可能有用的状态数据。
1) 功能
statePrefetcher 的主要功能是在执行区块之前，从磁盘中预先加载（预取）状态数据。这样，当主区块处理器实际开始执行时，可以减少磁盘I/O操作，提高执行效率。这种预取操作是在任意状态之上盲目执行区块的，也就是说它并不依赖于特定的状态，而是广泛地加载可能会使用到的数据。
2) 适用场景
该结构体和预取功能适用于需要在处理区块时提高效率的场景，特别是在区块链系统中，预取状态数据可以减少延迟，提高整体性能。
通过这种预取机制，可以显著减少处理区块时的瓶颈，提高区块链网络的吞吐量和响应速度。
*/

// statePrefetcher is a basic Prefetcher, which blindly executes a block on top
// of an arbitrary state with the goal of prefetching potentially useful state
// data from disk before the main block processor start executing.
type statePrefetcher struct {
	config *params.ChainConfig // Chain configuration options
	bc     *BlockChain         // Canonical block chain
	engine consensus.Engine    // Consensus engine used for block rewards
}

// NewStatePrefetcher initialises a new statePrefetcher.
func NewStatePrefetcher(config *params.ChainConfig, bc *BlockChain, engine consensus.Engine) *statePrefetcher {
	return &statePrefetcher{
		config: config,
		bc:     bc,
		engine: engine,
	}
}

// Prefetch processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb, but any changes are discarded. The
// only goal is to pre-cache transaction signatures and state trie nodes.
func (p *statePrefetcher) Prefetch(block *types.Block, statedb *state.StateDB, cfg *vm.Config, interruptCh <-chan struct{}) {
	var (
		header = block.Header()
		signer = types.MakeSigner(p.config, header.Number, header.Time)
	)
	transactions := block.Transactions()
	txChan := make(chan int, prefetchThread)
	// No need to execute the first batch, since the main processor will do it.
	for i := 0; i < prefetchThread; i++ {
		go func() {
			newStatedb := statedb.CopyDoPrefetch()
			if !p.config.IsHertzfix(header.Number) {
				newStatedb.EnableWriteOnSharedStorage()
			}
			gaspool := new(GasPool).AddGas(block.GasLimit())
			blockContext := NewEVMBlockContext(header, p.bc, nil)
			evm := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, *cfg)
			// Iterate over and process the individual transactions
			for {
				select {
				case txIndex := <-txChan:
					tx := transactions[txIndex]
					// Convert the transaction into an executable message and pre-cache its sender
					msg, err := TransactionToMessage(tx, signer, header.BaseFee)
					msg.SkipAccountChecks = true
					if err != nil {
						return // Also invalid block, bail out
					}
					newStatedb.SetTxContext(tx.Hash(), txIndex)
					precacheTransaction(msg, p.config, gaspool, newStatedb, header, evm)

				case <-interruptCh:
					// If block precaching was interrupted, abort
					return
				}
			}
		}()
	}

	// it should be in a separate goroutine, to avoid blocking the critical path.
	for i := 0; i < len(transactions); i++ {
		select {
		case txChan <- i:
		case <-interruptCh:
			return
		}
	}
}

// PrefetchMining processes the state changes according to the Ethereum rules by running
// the transaction messages using the statedb, but any changes are discarded. The
// only goal is to pre-cache transaction signatures and snapshot clean state. Only used for mining stage
func (p *statePrefetcher) PrefetchMining(txs TransactionsByPriceAndNonce, header *types.Header, gasLimit uint64, statedb *state.StateDB, cfg vm.Config, interruptCh <-chan struct{}, txCurr **types.Transaction) {
	var signer = types.MakeSigner(p.config, header.Number, header.Time)

	txCh := make(chan *types.Transaction, 2*prefetchThread)
	for i := 0; i < prefetchThread; i++ {
		go func(startCh <-chan *types.Transaction, stopCh <-chan struct{}) {
			idx := 0
			newStatedb := statedb.CopyDoPrefetch()
			if !p.config.IsHertzfix(header.Number) {
				newStatedb.EnableWriteOnSharedStorage()
			}
			gaspool := new(GasPool).AddGas(gasLimit)
			blockContext := NewEVMBlockContext(header, p.bc, nil)
			evm := vm.NewEVM(blockContext, vm.TxContext{}, statedb, p.config, cfg)
			// Iterate over and process the individual transactions
			for {
				select {
				case tx := <-startCh:
					// Convert the transaction into an executable message and pre-cache its sender
					msg, err := TransactionToMessage(tx, signer, header.BaseFee)
					msg.SkipAccountChecks = true
					if err != nil {
						return // Also invalid block, bail out
					}
					idx++
					newStatedb.SetTxContext(tx.Hash(), idx)
					precacheTransaction(msg, p.config, gaspool, newStatedb, header, evm)
					gaspool = new(GasPool).AddGas(gasLimit)
				case <-stopCh:
					return
				}
			}
		}(txCh, interruptCh)
	}
	go func(txset TransactionsByPriceAndNonce) {
		count := 0
		for {
			select {
			case <-interruptCh:
				return
			default:
				if count++; count%checkInterval == 0 {
					txset.Forward(*txCurr)
				}
				tx := txset.PeekWithUnwrap()
				if tx == nil {
					return
				}

				select {
				case <-interruptCh:
					return
				case txCh <- tx:
				}

				txset.Shift()
			}
		}
	}(txs)
}

// precacheTransaction attempts to apply a transaction to the given state database
// and uses the input parameters for its environment. The goal is not to execute
// the transaction successfully, rather to warm up touched data slots.
func precacheTransaction(msg *Message, config *params.ChainConfig, gaspool *GasPool, statedb *state.StateDB, header *types.Header, evm *vm.EVM) error {
	// Update the evm with the new transaction context.
	evm.Reset(NewEVMTxContext(msg), statedb)
	// Add addresses to access list if applicable
	_, err := ApplyMessage(evm, msg, gaspool)
	if err == nil {
		statedb.Finalise(true)
	}
	return err
}
