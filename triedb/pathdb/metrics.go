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
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>

package pathdb

import "github.com/ethereum/go-ethereum/metrics"

var (
	/*
		cleanHitMeter
		dirtyHitMeter
		commitTimeTimer
	*/
	/*
		cleanHitMeter: 用在disklayer.go文件中Node函数里，如果fastcache缓存里有对应的node data，标记一下，`cleanHitMeter.Mark(1)`
		cleanMissMeter: 用在disklayer.go文件中Node函数里，如果fastcache缓存里没有对应的node data，标记一下，`cleanMissMeter.Mark(1)`
		cleanReadMeter: 用在disklayer.go文件中Node函数里，如果fastcache缓存里有对应的node data，记录数据大小，`cleanReadMeter.Mark(int64(len(blob)))`
		cleanWriteMeter:用在disklayer.go文件中Node函数里，将从磁盘里读到的数据缓存在fastcache中，标记一下数据大小， `cleanWriteMeter.Mark(int64(len(nBlob)))`
	*/
	cleanHitMeter   = metrics.NewRegisteredMeter("pathdb/clean/hit", nil)
	cleanMissMeter  = metrics.NewRegisteredMeter("pathdb/clean/miss", nil)
	cleanReadMeter  = metrics.NewRegisteredMeter("pathdb/clean/read", nil)
	cleanWriteMeter = metrics.NewRegisteredMeter("pathdb/clean/write", nil)

	/*
		dirtyHitMeter: 用在difflayer.go文件node函数中，当成功检索到node data时，`dirtyHitMeter.Mark(1)`；用在disklayer.go文件中Node函数里，如果nodebuffer里有相关要检索的数据就标记为1，`dirtyHitMeter.Mark(1)`
		dirtyMissMeter:用在disklayer.go文件中Node函数里，如果nodebuffer里没有检索到对应的node data，标记一下，`dirtyMissMeter.Mark(1)`
		dirtyReadMeter:
		1) 用在difflayer.go文件node函数中，记录读取到的blob数据大小，`dirtyReadMeter.Mark(int64(len(n.Blob)))`；
		2) 用在disklayer.go文件中Node函数里，如果nodebuffer里有相关要检索的数据就记录读到的blob数据大小。dirtyReadMeter.Mark(int64(len(n.Blob)))
		dirtyWriteMeter: 用在newDiffLayer函数中，记录node.Blob和path的总大小，`dirtyWriteMeter.Mark(size)`
		dirtyNodeHitDepthHist: 用在difflayer.go文件node函数中，当成功检索到node data时的深度，`Update(int64(depth))`
	*/
	dirtyHitMeter         = metrics.NewRegisteredMeter("pathdb/dirty/hit", nil)
	dirtyMissMeter        = metrics.NewRegisteredMeter("pathdb/dirty/miss", nil)
	dirtyReadMeter        = metrics.NewRegisteredMeter("pathdb/dirty/read", nil)
	dirtyWriteMeter       = metrics.NewRegisteredMeter("pathdb/dirty/write", nil)
	dirtyNodeHitDepthHist = metrics.NewRegisteredHistogram("pathdb/dirty/depth", nil, metrics.NewExpDecaySample(1028, 0.015))

	/*
		cleanFalseMeter:用在disklayer.go文件中Node函数里，如何fastcache中缓存的数据计算出的hash与指定的hash不匹配，记录一下，`cleanFalseMeter.Mark(1)`
		dirtyFalseMeter:用在difflayer.go文件node函数中，记录当一个trieNode owner和path匹配，但是hash不匹配，`dirtyFalseMeter.Mark(1)`；nodebuffer.go文件中的node函数也使用了该metrics，用法类似
		diskFalseMeter:用在disklayer.go文件中Node函数里，如果从磁盘上读到的数据计算出的hash与指定的hash不匹配，记录一下，`diskFalseMeter.Mark(1)`
	*/
	cleanFalseMeter = metrics.NewRegisteredMeter("pathdb/clean/false", nil)
	dirtyFalseMeter = metrics.NewRegisteredMeter("pathdb/dirty/false", nil)
	diskFalseMeter  = metrics.NewRegisteredMeter("pathdb/disk/false", nil)

	/*
		commitTimeTimer:用在nodebuffer.go文件flush函数中，记录提交的size，`commitBytesMeter.Mark(int64(size))`
		commitNodesMeter:用在nodebuffer.go文件flush函数中，记录提交的nodes数量，`commitNodesMeter.Mark(int64(nodes))`
		commitBytesMeter:用在nodebuffer.go文件flush函数中，记录向数据库中提交数据花费的时间，`commitTimeTimer.UpdateSince(start)`
	*/
	commitTimeTimer  = metrics.NewRegisteredTimer("pathdb/commit/time", nil)
	commitNodesMeter = metrics.NewRegisteredMeter("pathdb/commit/nodes", nil)
	commitBytesMeter = metrics.NewRegisteredMeter("pathdb/commit/bytes", nil)

	/*
		gcNodesMeter:用在nodebuffer.go文件commit函数中，记录提交的nodes数量，`gcNodesMeter.Mark(overwrite)`
		gcBytesMeter:用在nodebuffer.go文件commit函数中，记录提交的nodes大小，`gcBytesMeter.Mark(overwriteSize)`
	*/
	gcNodesMeter = metrics.NewRegisteredMeter("pathdb/gc/nodes", nil)
	gcBytesMeter = metrics.NewRegisteredMeter("pathdb/gc/bytes", nil)

	/*
		diffLayerBytesMeter:用在newDiffLayer函数中，记录nodes参数占用的总内存：uint64(n.Size() + len(path))，n.Size()包括hash长度，`diffLayerBytesMeter.Mark(int64(dl.memory))`
		diffLayerNodesMeter:用在newDiffLayer函数中，记录nodes(map[common.Hash]map[string]*trienode.Node)参数key的数量，`diffLayerNodesMeter.Mark(int64(count))`
	*/
	diffLayerBytesMeter = metrics.NewRegisteredMeter("pathdb/diff/bytes", nil)
	diffLayerNodesMeter = metrics.NewRegisteredMeter("pathdb/diff/nodes", nil)

	/*
		historyBuildTimeMeter:用在history.go文件writeHistory函数中用来记录该函数的执行时间，`historyBuildTimeMeter.UpdateSince(start)`
		historyDataBytesMeter:用在history.go文件writeHistory函数中用来记录写到db层的data size，`historyDataBytesMeter.Mark(int64(dataSize))`
		historyIndexBytesMeter:用在history.go文件writeHistory函数中用来记录写到db层的index size，`historyIndexBytesMeter.Mark(int64(indexSize))`
	*/
	historyBuildTimeMeter  = metrics.NewRegisteredTimer("pathdb/history/time", nil)
	historyDataBytesMeter  = metrics.NewRegisteredMeter("pathdb/history/bytes/data", nil)
	historyIndexBytesMeter = metrics.NewRegisteredMeter("pathdb/history/bytes/index", nil)
)
