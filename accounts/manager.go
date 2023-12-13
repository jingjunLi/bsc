// Copyright 2017 The go-ethereum Authors
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

package accounts

import (
	"reflect"
	"sort"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/event"
)

// managerSubBufferSize determines how many incoming wallet events
// the manager will buffer in its channel.
const managerSubBufferSize = 50

// Config contains the settings of the global account manager.
//
// TODO(rjl493456442, karalabe, holiman): Get rid of this when account management
// is removed in favor of Clef.
type Config struct {
	InsecureUnlockAllowed bool // Whether account unlocking in insecure environment is allowed
}

// newBackendEvent lets the manager know it should
// track the given backend for wallet updates.
type newBackendEvent struct {
	backend   Backend
	processed chan struct{} // Informs event emitter that backend has been integrated
}

// Manager is an overarching account manager that can communicate with various
// backends for signing transactions.
/*
Manager 是一个包含所有东西的账户管理工具。 可以和所有的 Backends 来通信来签署交易。
*/
type Manager struct {
	config *Config // Global account manager configurations
	// 所有已经注册的 Backend
	backends map[reflect.Type][]Backend // Index of backends currently registered
	// 所有 Backend 的更新订阅器
	updaters []event.Subscription // Wallet update subscriptions for all backends
	// backend 更新的订阅槽 是一个channel类型，用于接收钱包相关的事件。Manager需要调用backend的 Subscribe() 函数把这个channel注册到后端中去。
	updates     chan WalletEvent     // Subscription sink for backend wallet changes
	newBackends chan newBackendEvent // Incoming backends to be tracked by the manager
	// 所有已经注册的 Backends 的钱包的缓存
	wallets []Wallet // Cache of all wallets from all registered backends

	// 钱包到达和离开的通知
	feed event.Feed // Wallet feed notifying of arrivals/departures

	// 退出队列
	quit chan chan error
	term chan struct{} // Channel is closed upon termination of the update loop
	lock sync.RWMutex
}

// NewManager creates a generic account manager to sign transaction via various
// supported backends.
/*
NewManager 创建通用的 account manager , 通过 各种 backends 对 transaction 进行签名

1) 调用所有 backend 的 Wallets() 方法，合并成完整的钱包列表
2) 创建channel，并调用所有 backend 的 Subscribe() 函数进行注册
3) 初始化 Manager 实例，调用 update() 函数进入钱包事件监听循环
*/
func NewManager(config *Config, backends ...Backend) *Manager {
	// Retrieve the initial list of wallets from the backends and sort by URL
	var wallets []Wallet
	for _, backend := range backends {
		wallets = merge(wallets, backend.Wallets()...)
	}
	// Subscribe to wallet notifications from all backends
	updates := make(chan WalletEvent, managerSubBufferSize)

	subs := make([]event.Subscription, len(backends))
	for i, backend := range backends {
		subs[i] = backend.Subscribe(updates)
	}
	// Assemble the account manager and return
	am := &Manager{
		config:      config,
		backends:    make(map[reflect.Type][]Backend),
		updaters:    subs,
		updates:     updates,
		newBackends: make(chan newBackendEvent),
		wallets:     wallets,
		quit:        make(chan chan error),
		term:        make(chan struct{}),
	}
	for _, backend := range backends {
		kind := reflect.TypeOf(backend)
		am.backends[kind] = append(am.backends[kind], backend)
	}
	go am.update()

	return am
}

// Close terminates the account manager's internal notification processes.
func (am *Manager) Close() error {
	errc := make(chan error)
	am.quit <- errc
	return <-errc
}

// Config returns the configuration of account manager.
func (am *Manager) Config() *Config {
	return am.config
}

// AddBackend starts the tracking of an additional backend for wallet updates.
// cmd/geth assumes once this func returns the backends have been already integrated.
/*
生成 newBackendEvent 通知 newBackends
*/
func (am *Manager) AddBackend(backend Backend) {
	done := make(chan struct{})
	am.newBackends <- newBackendEvent{backend, done}
	<-done
}

// update is the wallet event loop listening for notifications from the backends
// and updating the cache of wallets.
/*
update 方法。 是一个 goroutine 。会监听所有 backend 触发的更新信息。 然后转发给 feed.
这就是一个无限循环，监听后端发送过来的钱包事件。
细心的朋友可能发现Manager也有个feed字段，而且还有个Subscribe()函数，这个是干什么用的呢？其实是为了把钱包事件再转发给上层的订阅者，也就是Node。订阅代码参见cmd/geth/main.go中的startNode()函数：
*/
func (am *Manager) update() {
	// Close all subscriptions when the manager terminates
	defer func() {
		am.lock.Lock()
		for _, sub := range am.updaters {
			sub.Unsubscribe()
		}
		am.updaters = nil
		am.lock.Unlock()
	}()

	// Loop until termination
	/*
		监听三种 event: 1) updates WalletEvent; 2) newBackends newBackendEvent 3) quit
	*/
	for {
		select {
		case event := <-am.updates:
			// Wallet event arrived, update local cache
			am.lock.Lock()
			switch event.Kind {
			case WalletArrived:
				am.wallets = merge(am.wallets, event.Wallet)
			case WalletDropped:
				am.wallets = drop(am.wallets, event.Wallet)
			}
			am.lock.Unlock()

			// Notify any listeners of the event
			am.feed.Send(event)
		case event := <-am.newBackends:
			am.lock.Lock()
			// Update caches
			backend := event.backend
			am.wallets = merge(am.wallets, backend.Wallets()...)
			am.updaters = append(am.updaters, backend.Subscribe(am.updates))
			kind := reflect.TypeOf(backend)
			am.backends[kind] = append(am.backends[kind], backend)
			am.lock.Unlock()
			close(event.processed)
		case errc := <-am.quit:
			// Manager terminating, return
			errc <- nil
			// Signals event emitters the loop is not receiving values
			// to prevent them from getting stuck.
			close(am.term)
			return
		}
	}
}

// Backends retrieves the backend(s) with the given type from the account manager.
// 返回 backend
func (am *Manager) Backends(kind reflect.Type) []Backend {
	am.lock.RLock()
	defer am.lock.RUnlock()

	return am.backends[kind]
}

// Wallets returns all signer accounts registered under this account manager.
func (am *Manager) Wallets() []Wallet {
	am.lock.RLock()
	defer am.lock.RUnlock()

	return am.walletsNoLock()
}

// walletsNoLock returns all registered wallets. Callers must hold am.lock.
func (am *Manager) walletsNoLock() []Wallet {
	cpy := make([]Wallet, len(am.wallets))
	copy(cpy, am.wallets)
	return cpy
}

// Wallet retrieves the wallet associated with a particular URL.
func (am *Manager) Wallet(url string) (Wallet, error) {
	am.lock.RLock()
	defer am.lock.RUnlock()

	parsed, err := parseURL(url)
	if err != nil {
		return nil, err
	}
	for _, wallet := range am.walletsNoLock() {
		if wallet.URL() == parsed {
			return wallet, nil
		}
	}
	return nil, ErrUnknownWallet
}

// Accounts returns all account addresses of all wallets within the account manager
func (am *Manager) Accounts() []common.Address {
	am.lock.RLock()
	defer am.lock.RUnlock()

	addresses := make([]common.Address, 0) // return [] instead of nil if empty
	for _, wallet := range am.wallets {
		for _, account := range wallet.Accounts() {
			addresses = append(addresses, account.Address)
		}
	}
	return addresses
}

// Find attempts to locate the wallet corresponding to a specific account. Since
// accounts can be dynamically added to and removed from wallets, this method has
// a linear runtime in the number of wallets.
func (am *Manager) Find(account Account) (Wallet, error) {
	am.lock.RLock()
	defer am.lock.RUnlock()

	for _, wallet := range am.wallets {
		if wallet.Contains(account) {
			return wallet, nil
		}
	}
	return nil, ErrUnknownAccount
}

// Subscribe creates an async subscription to receive notifications when the
// manager detects the arrival or departure of a wallet from any of its backends.
// 订阅消息
func (am *Manager) Subscribe(sink chan<- WalletEvent) event.Subscription {
	return am.feed.Subscribe(sink)
}

// merge is a sorted analogue of append for wallets, where the ordering of the
// origin list is preserved by inserting new wallets at the correct position.
//
// The original slice is assumed to be already sorted by URL.
func merge(slice []Wallet, wallets ...Wallet) []Wallet {
	for _, wallet := range wallets {
		n := sort.Search(len(slice), func(i int) bool { return slice[i].URL().Cmp(wallet.URL()) >= 0 })
		if n == len(slice) {
			slice = append(slice, wallet)
			continue
		}
		slice = append(slice[:n], append([]Wallet{wallet}, slice[n:]...)...)
	}
	return slice
}

// drop is the counterpart of merge, which looks up wallets from within the sorted
// cache and removes the ones specified.
func drop(slice []Wallet, wallets ...Wallet) []Wallet {
	for _, wallet := range wallets {
		n := sort.Search(len(slice), func(i int) bool { return slice[i].URL().Cmp(wallet.URL()) >= 0 })
		if n == len(slice) {
			// Wallet not found, may happen during startup
			continue
		}
		slice = append(slice[:n], slice[n+1:]...)
	}
	return slice
}
