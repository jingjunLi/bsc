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

package types

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
)

type bytesBacked interface {
	Bytes() []byte
}

const (
	// BloomByteLength represents the number of bytes used in a header log bloom.
	BloomByteLength = 256

	// BloomBitLength represents the number of bits used in a header log bloom.
	BloomBitLength = 8 * BloomByteLength
)

var EmptyBloom = Bloom{}

// Bloom represents a 2048 bit bloom filter.
type Bloom [BloomByteLength]byte

// BytesToBloom converts a byte slice to a bloom filter.
// It panics if b is not of suitable size.
func BytesToBloom(b []byte) Bloom {
	var bloom Bloom
	bloom.SetBytes(b)
	return bloom
}

// SetBytes sets the content of b to the given bytes.
// It panics if d is not of suitable size.
func (b *Bloom) SetBytes(d []byte) {
	if len(b) < len(d) {
		panic(fmt.Sprintf("bloom bytes too big %d %d", len(b), len(d)))
	}
	copy(b[BloomByteLength-len(d):], d)
}

// Add adds d to the filter. Future calls of Test(d) will return true.
func (b *Bloom) Add(d []byte) {
	b.add(d, make([]byte, 6))
}

// add is internal version of Add, which takes a scratch buffer for reuse (needs to be at least 6 bytes)
func (b *Bloom) add(d []byte, buf []byte) {
	i1, v1, i2, v2, i3, v3 := bloomValues(d, buf)
	b[i1] |= v1
	b[i2] |= v2
	b[i3] |= v3
}

// Big converts b to a big integer.
// Note: Converting a bloom filter to a big.Int and then calling GetBytes
// does not return the same bytes, since big.Int will trim leading zeroes
func (b Bloom) Big() *big.Int {
	return new(big.Int).SetBytes(b[:])
}

// Bytes returns the backing byte slice of the bloom
func (b Bloom) Bytes() []byte {
	return b[:]
}

// Test checks if the given topic is present in the bloom filter
func (b Bloom) Test(topic []byte) bool {
	i1, v1, i2, v2, i3, v3 := bloomValues(topic, make([]byte, 6))
	return v1 == v1&b[i1] &&
		v2 == v2&b[i2] &&
		v3 == v3&b[i3]
}

// MarshalText encodes b as a hex string with 0x prefix.
func (b Bloom) MarshalText() ([]byte, error) {
	return hexutil.Bytes(b[:]).MarshalText()
}

// UnmarshalText b as a hex string with 0x prefix.
func (b *Bloom) UnmarshalText(input []byte) error {
	return hexutil.UnmarshalFixedText("Bloom", input, b[:])
}

// CreateBloom creates a bloom filter out of the give Receipts (+Logs)
/*
CreateBloom(receipts Receipts) 方法创建收据的 bloom
3.1，创建一个空的bigInt bloomBin，遍历receipts，取得receipt里的日志，调用LogsBloom(receipt.Logs)将取得所有日志的bloom值按位或和到bloomBin。这意味着bloomBin包括了所有日志的bloom9数据。
3.2，调用BytesToBloom(bloomBin.Bytes())方法，把bloomBin加入区块的bloom过滤器中，这时Bloom过滤器就有了本次交易的所有收据。
3.3，需要说明的是Bloom过滤器只是提供一个查找数据是否存在的工具，它本身不包含任何数据。

给定 Receipts, 将其内的 所有 Log 中的 Topics 存入到 Bloom 中. 两层遍历;
*/
func CreateBloom(receipts Receipts) Bloom {
	buf := make([]byte, 6)
	var bin Bloom
	for _, receipt := range receipts {
		for _, log := range receipt.Logs {
			bin.add(log.Address.Bytes(), buf)
			for _, b := range log.Topics {
				bin.add(b[:], buf)
			}
		}
	}
	return bin
}

// LogsBloom returns the bloom bytes for the given logs
// 把日志数据转成对应的 bloom9 值，包括日志的合约地址以及每个日志 Topic
func LogsBloom(logs []*Log) []byte {
	buf := make([]byte, 6)
	var bin Bloom
	for _, log := range logs {
		bin.add(log.Address.Bytes(), buf)
		for _, b := range log.Topics {
			bin.add(b[:], buf)
		}
	}
	return bin[:]
}

// Bloom9 returns the bloom filter for the given data
func Bloom9(data []byte) []byte {
	var b Bloom
	b.SetBytes(data)
	return b.Bytes()
}

// bloomValues returns the bytes (index-value pairs) to set for the given data
/*
先看看 bloom9(b []byte) 算法函数。
1.1 首先将传入的数据，进行hash256的运算，得到一个32字节的hash
1.2 然后取第0和第1字节的值合成一个2字节无符号的int，和2047做按位与运算，得到一个小于2048的值b，这个值就表示bloom里面第b位的值为1。同理取第2,3 和第4,5字节合成另外两个无符号int，增加在bloom里面的命中率。
1.3 也就是说对于任何一个输入，如果它对应的三个下标的值不都为1，那么它肯定不在这个区块中。 当如如果对应的三位都为1，也不能说明一定在这个区块中。 这就是布隆过滤器的特性。
1.4 这三个数取或，得到一个bigInt，代表这个传参数据的 bloom9 值。
*/
func bloomValues(data []byte, hashbuf []byte) (uint, byte, uint, byte, uint, byte) {
	sha := hasherPool.Get().(crypto.KeccakState)
	sha.Reset()
	sha.Write(data)
	sha.Read(hashbuf)
	hasherPool.Put(sha)
	// The actual bits to flip
	v1 := byte(1 << (hashbuf[1] & 0x7))
	v2 := byte(1 << (hashbuf[3] & 0x7))
	v3 := byte(1 << (hashbuf[5] & 0x7))
	// The indices for the bytes to OR in
	i1 := BloomByteLength - uint((binary.BigEndian.Uint16(hashbuf)&0x7ff)>>3) - 1
	i2 := BloomByteLength - uint((binary.BigEndian.Uint16(hashbuf[2:])&0x7ff)>>3) - 1
	i3 := BloomByteLength - uint((binary.BigEndian.Uint16(hashbuf[4:])&0x7ff)>>3) - 1

	return i1, v1, i2, v2, i3, v3
}

// BloomLookup is a convenience-method to check presence in the bloom filter
// 给定 topic, 查询其是否在 bloom 中
func BloomLookup(bin Bloom, topic bytesBacked) bool {
	return bin.Test(topic.Bytes())
}
