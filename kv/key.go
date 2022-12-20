// Copyright 2021 TiKV Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// NOTE: The code in this file is based on code from the
// TiDB project, licensed under the Apache License v 2.0
//
// https://github.com/pingcap/tidb/tree/cc5e161ac06827589c4966674597c137cc9e809c/store/tikv/kv/key.go
//

// Copyright 2021 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package kv

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"sort"
	"unsafe"

	"github.com/pingcap/kvproto/pkg/coprocessor"
)

// NextKey returns the next key in byte-order.
func NextKey(k []byte) []byte {
	// add 0x0 to the end of key
	buf := make([]byte, len(k)+1)
	copy(buf, k)
	return buf
}

// PrefixNextKey returns the next prefix key.
//
// Assume there are keys like:
//
//	rowkey1
//	rowkey1_column1
//	rowkey1_column2
//	rowKey2
//
// If we seek 'rowkey1' NextKey, we will get 'rowkey1_column1'.
// If we seek 'rowkey1' PrefixNextKey, we will get 'rowkey2'.
func PrefixNextKey(k []byte) []byte {
	buf := make([]byte, len(k))
	copy(buf, k)
	var i int
	for i = len(k) - 1; i >= 0; i-- {
		buf[i]++
		if buf[i] != 0 {
			break
		}
	}
	if i == -1 {
		// Unlike TiDB, for the specific key 0xFF
		// we return empty slice instead of {0xFF, 0x0}
		buf = make([]byte, 0)
	}
	return buf
}

// CmpKey returns the comparison result of two key.
// The result will be 0 if a==b, -1 if a < b, and +1 if a > b.
func CmpKey(k, another []byte) int {
	return bytes.Compare(k, another)
}

// StrKey returns string for key.
func StrKey(k []byte) string {
	return hex.EncodeToString(k)
}

// KeyRange represents a range where StartKey <= key < EndKey.
type KeyRange struct {
	StartKey []byte
	EndKey   []byte
}

// IsPoint checks if the key range represents a point.
func (r *KeyRange) IsPoint() bool {
	if len(r.StartKey) != len(r.EndKey) {
		// Works like
		//   return bytes.Equal(r.StartKey.Next(), r.EndKey)

		startLen := len(r.StartKey)
		return startLen+1 == len(r.EndKey) &&
			r.EndKey[startLen] == 0 &&
			bytes.Equal(r.StartKey, r.EndKey[:startLen])
	}
	// Works like
	//   return bytes.Equal(r.StartKey.PrefixNext(), r.EndKey)

	i := len(r.StartKey) - 1
	for ; i >= 0; i-- {
		if r.StartKey[i] != 255 {
			break
		}
		if r.EndKey[i] != 0 {
			return false
		}
	}
	if i < 0 {
		// In case all bytes in StartKey are 255.
		return false
	}
	// The byte at diffIdx in StartKey should be one less than the byte at diffIdx in EndKey.
	// And bytes in StartKey and EndKey before diffIdx should be equal.
	diffOneIdx := i
	return r.StartKey[diffOneIdx]+1 == r.EndKey[diffOneIdx] &&
		bytes.Equal(r.StartKey[:diffOneIdx], r.EndKey[:diffOneIdx])
}

// KeyRanges is like []kv.KeyRange, but may has extra elements at head/tail.
// It's for avoiding alloc big slice during build copTask.
type KeyRanges struct {
	First *KeyRange
	Mid   []KeyRange
	Last  *KeyRange
}

// NewKeyRanges constructs a KeyRanges instance.
func NewKeyRanges(ranges []KeyRange) *KeyRanges {
	return &KeyRanges{Mid: ranges}
}

func (r *KeyRanges) String() string {
	var s string
	r.Do(func(ran *KeyRange) {
		s += fmt.Sprintf("[%q, %q]", ran.StartKey, ran.EndKey)
	})
	return s
}

// Len returns the count of ranges.
func (r *KeyRanges) Len() int {
	var l int
	if r.First != nil {
		l++
	}
	l += len(r.Mid)
	if r.Last != nil {
		l++
	}
	return l
}

// At returns the range at the ith position.
func (r *KeyRanges) At(i int) KeyRange {
	if r.First != nil {
		if i == 0 {
			return *r.First
		}
		i--
	}
	if i < len(r.Mid) {
		return r.Mid[i]
	}
	return *r.Last
}

// Slice returns the sub ranges [from, to).
func (r *KeyRanges) Slice(from, to int) *KeyRanges {
	var ran KeyRanges
	if r.First != nil {
		if from == 0 && to > 0 {
			ran.First = r.First
		}
		if from > 0 {
			from--
		}
		if to > 0 {
			to--
		}
	}
	if to <= len(r.Mid) {
		ran.Mid = r.Mid[from:to]
	} else {
		if from <= len(r.Mid) {
			ran.Mid = r.Mid[from:]
		}
		if from < to {
			ran.Last = r.Last
		}
	}
	return &ran
}

// Do applies a functions to all ranges.
func (r *KeyRanges) Do(f func(ran *KeyRange)) {
	if r.First != nil {
		f(r.First)
	}
	for i := range r.Mid {
		f(&r.Mid[i])
	}
	if r.Last != nil {
		f(r.Last)
	}
}

// Split ranges into (left, right) by key.
func (r *KeyRanges) Split(key []byte) (*KeyRanges, *KeyRanges) {
	n := sort.Search(r.Len(), func(i int) bool {
		cur := r.At(i)
		return len(cur.EndKey) == 0 || bytes.Compare(cur.EndKey, key) > 0
	})
	// If a range p contains the key, it will split to 2 parts.
	if n < r.Len() {
		p := r.At(n)
		if bytes.Compare(key, p.StartKey) > 0 {
			left := r.Slice(0, n)
			left.Last = &KeyRange{StartKey: p.StartKey, EndKey: key}
			right := r.Slice(n+1, r.Len())
			right.First = &KeyRange{StartKey: key, EndKey: p.EndKey}
			return left, right
		}
	}
	return r.Slice(0, n), r.Slice(n, r.Len())
}

// ToPBRanges converts ranges to wire type.
func (r *KeyRanges) ToPBRanges() []*coprocessor.KeyRange {
	ranges := make([]*coprocessor.KeyRange, 0, r.Len())
	r.Do(func(ran *KeyRange) {
		// kv.KeyRange and coprocessor.KeyRange are the same,
		// so use unsafe.Pointer to avoid allocation here.
		ranges = append(ranges, (*coprocessor.KeyRange)(unsafe.Pointer(ran)))
	})
	return ranges
}
