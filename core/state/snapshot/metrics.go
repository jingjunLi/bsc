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

package snapshot

import "github.com/ethereum/go-ethereum/metrics"

// Metrics in generation
var (
	snapGeneratedAccountMeter     = metrics.NewRegisteredMeter("state/snapshot/generation/account/generated", nil)
	snapRecoveredAccountMeter     = metrics.NewRegisteredMeter("state/snapshot/generation/account/recovered", nil)
	snapWipedAccountMeter         = metrics.NewRegisteredMeter("state/snapshot/generation/account/wiped", nil)
	snapMissallAccountMeter       = metrics.NewRegisteredMeter("state/snapshot/generation/account/missall", nil)
	snapGeneratedStorageMeter     = metrics.NewRegisteredMeter("state/snapshot/generation/storage/generated", nil)
	snapRecoveredStorageMeter     = metrics.NewRegisteredMeter("state/snapshot/generation/storage/recovered", nil)
	snapWipedStorageMeter         = metrics.NewRegisteredMeter("state/snapshot/generation/storage/wiped", nil)
	snapMissallStorageMeter       = metrics.NewRegisteredMeter("state/snapshot/generation/storage/missall", nil)
	snapDanglingStorageMeter      = metrics.NewRegisteredMeter("state/snapshot/generation/storage/dangling", nil)
	snapSuccessfulRangeProofMeter = metrics.NewRegisteredMeter("state/snapshot/generation/proof/success", nil)
	snapFailedRangeProofMeter     = metrics.NewRegisteredMeter("state/snapshot/generation/proof/failure", nil)

	// snapAccountProveCounter measures time spent on the account proving
	snapAccountProveCounter = metrics.NewRegisteredCounter("state/snapshot/generation/duration/account/prove", nil)
	// snapAccountTrieReadCounter measures time spent on the account trie iteration
	snapAccountTrieReadCounter = metrics.NewRegisteredCounter("state/snapshot/generation/duration/account/trieread", nil)
	// snapAccountSnapReadCounter measures time spent on the snapshot account iteration
	snapAccountSnapReadCounter = metrics.NewRegisteredCounter("state/snapshot/generation/duration/account/snapread", nil)
	// snapAccountWriteCounter measures time spent on writing/updating/deleting accounts
	snapAccountWriteCounter = metrics.NewRegisteredCounter("state/snapshot/generation/duration/account/write", nil)
	// snapStorageProveCounter measures time spent on storage proving
	snapStorageProveCounter = metrics.NewRegisteredCounter("state/snapshot/generation/duration/storage/prove", nil)
	// snapStorageTrieReadCounter measures time spent on the storage trie iteration
	snapStorageTrieReadCounter = metrics.NewRegisteredCounter("state/snapshot/generation/duration/storage/trieread", nil)
	// snapStorageSnapReadCounter measures time spent on the snapshot storage iteration
	snapStorageSnapReadCounter = metrics.NewRegisteredCounter("state/snapshot/generation/duration/storage/snapread", nil)
	// snapStorageWriteCounter measures time spent on writing/updating storages
	snapStorageWriteCounter = metrics.NewRegisteredCounter("state/snapshot/generation/duration/storage/write", nil)
	// snapStorageCleanCounter measures time spent on deleting storages
	snapStorageCleanCounter = metrics.NewRegisteredCounter("state/snapshot/generation/duration/storage/clean", nil)

	snapLayersGuage = metrics.NewRegisteredGauge("pathdb/snaps/layers", nil)

	lookupAddSnapshotTimer      = metrics.NewRegisteredResettingTimer("pathdb/lookup/addSnapshot/time", nil)
	lookupRemoveSnapshotTimer   = metrics.NewRegisteredResettingTimer("pathdb/lookup/removeSnapshot/time", nil)
	lookupAddSnapshotCounter    = metrics.NewRegisteredMeter("pathdb/lookup/addSnapshot/added", nil)
	lookupRemoveSnapshotCounter = metrics.NewRegisteredMeter("pathdb/lookup/removeSnapshot/removed", nil)

	lookupAddLayerStorageTimer    = metrics.NewRegisteredResettingTimer("pathdb/lookup/addLayer/storage/time", nil)
	lookupAddLayerAccountTimer    = metrics.NewRegisteredResettingTimer("pathdb/lookup/addLayer/account/time", nil)
	lookupRemoveLayerStorageTimer = metrics.NewRegisteredResettingTimer("pathdb/lookup/removeLayer/storage/time", nil)
	lookupRemoveLayerAccountTimer = metrics.NewRegisteredResettingTimer("pathdb/lookup/removeLayer/account/time", nil)
	lookupAddDescendantTimer      = metrics.NewRegisteredResettingTimer("pathdb/lookup/addDescendant/time", nil)
	lookupRemoveDescendantTimer   = metrics.NewRegisteredResettingTimer("pathdb/lookup/removeDescendant/time", nil)

	//lookupAccountTimer             = metrics.NewRegisteredResettingTimer("snapshot/tree/api/LookupAccount", nil)
	lookupLookupAccountTimer       = metrics.NewRegisteredResettingTimer("snapshot/lookup/api/LookupAccount", nil)
	lookupLookupAccountNoLockTimer = metrics.NewRegisteredResettingTimer("snapshot/lookup/api/LookupAccountNoLock", nil)

	//lookupStorageTimer             = metrics.NewRegisteredResettingTimer("snapshot/tree/api/LookupStorage", nil)
	lookupLookupStorageTimer = metrics.NewRegisteredResettingTimer("snapshot/lookup/api/LookupStorage", nil)
	lookupStorageNoLockTimer = metrics.NewRegisteredResettingTimer("snapshot/lookup/api/LookupStorageNoLock", nil)

	lookupDescendantGauge = metrics.NewRegisteredGauge("pathdb/lookup/descendant", nil)
	lookupAccountGauge    = metrics.NewRegisteredGauge("pathdb/lookup/account", nil)
	lookupStorageGauge    = metrics.NewRegisteredGauge("pathdb/lookup/storage", nil)
	lookupLayersGauge     = metrics.NewRegisteredGauge("pathdb/lookup/layers", nil)

	lookupAccountListMaxValGauge = metrics.NewRegisteredGauge("pathdb/lookup/AccountListMaxVal", nil)
	lookupStorageListMaxValGauge = metrics.NewRegisteredGauge("pathdb/lookup/StorageListMaxVal", nil)

	lookupValueAccountGauge       = metrics.NewRegisteredGauge("pathdb/lookupValue/account", nil)
	lookupValueFirstStorageGauge  = metrics.NewRegisteredGauge("pathdb/lookupValue/firstStorage", nil)
	lookupValueSecondStorageGauge = metrics.NewRegisteredGauge("pathdb/lookupValue/secondStorage", nil)

	lookupAccountAppendIndexGauge = metrics.NewRegisteredGauge("pathdb/AppendIndex/account", nil)
	lookupStorageAppendIndexGauge = metrics.NewRegisteredGauge("pathdb/AppendIndex/storage", nil)

	//lookupAddLayerTimer      = metrics.NewRegisteredResettingTimer("pathdb/lookup/addLayer/time", nil)
	//lookupRemoveLayerTimer   = metrics.NewRegisteredResettingTimer("pathdb/lookup/removeLayer/time", nil)
	//lookupAddLayerCounter    = metrics.NewRegisteredMeter("pathdb/lookup/addLayer/added", nil)
	//lookupRemoveLayerCounter = metrics.NewRegisteredMeter("pathdb/lookup/removeLayer/removed", nil)

)
