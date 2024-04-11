package main

import (
	"encoding/binary"
	"errors"
	"net/netip"
	"slices"
)

var ErrNoFreePool = errors.New("no free address pools")

type Allocator struct {
	pools     []Pool
	allocated []netip.Prefix
}

type Pool struct {
	Prefix netip.Prefix
	Size   int
}

func NewAllocator(pools []Pool) *Allocator {
	for i, p := range pools {
		pools[i].Prefix = p.Prefix.Masked()
	}

	slices.SortFunc(pools, func(a, b Pool) int {
		addr := a.Prefix.Addr().Compare(b.Prefix.Addr())
		if addr < 0 || addr > 0 {
			return addr
		}

		if a.Prefix.Bits() < b.Prefix.Bits() {
			return -1
		} else if a.Prefix.Bits() > b.Prefix.Bits() {
			return 1
		}
		return 0
	})

	return &Allocator{
		pools:     pools,
		allocated: []netip.Prefix{},
	}
}

func (a *Allocator) Allocate() (netip.Prefix, error) {
	var i, poolID int
	var partialOverlap bool

	nextPrefixAfter := func(prev, p netip.Prefix, targetSz int) netip.Prefix {
		// TOOD: is there a simpler way? This code looks silly.
		next := nextPrefix(netip.PrefixFrom(prev.Addr(), targetSz).Masked())

		if p.Overlaps(next) {
			return next
		}

		return netip.Prefix{}
	}

	for i < len(a.allocated) {
		allocated := a.allocated[i]

		if poolID >= len(a.pools) {
			return netip.Prefix{}, ErrNoFreePool
		}

		p := a.pools[poolID]

		if allocated.Overlaps(p.Prefix) {
			i++

			if allocated.Bits() <= p.Prefix.Bits() {
				// The current 'allocated' prefix is bigger than the pool, thus
				// the pool is fully overlapped.
				partialOverlap = false
				poolID++
				continue
			}

			if lastAddr(allocated) == lastAddr(p.Prefix) {
				// The last address of the current 'allocated' prefix is the
				// same as the last address of the pool, it's fully overlapped.
				// We can go to the next one.
				partialOverlap = false
				poolID++
				continue
			}

			// This pool is partially overlapped. If the next iteration yields
			// an 'allocated' prefix that don't overlap with the current pool,
			// then might have found the right spot.
			partialOverlap = true
			continue
		}

		// Okay, so previous 'allocated' overlapped and current doesn't. Now
		// the question is: is there enough space left between previous
		// 'allocated' and the end of p?
		if partialOverlap {
			partialOverlap = false

			// No need to check if 'i > 0' -- the lowest 'i' where 'partialOverlap'
			// could be set is 1.
			prevAlloc := a.allocated[i-1]
			if next := nextPrefixAfter(prevAlloc, p.Prefix, p.Size); next != (netip.Prefix{}) {
				a.allocated = slices.Insert(a.allocated, i, next)
				return next, nil
			}

			// No luck -- nextPrefixAfter yielded an invalid prefix. There's
			// not enough space left to use this pool.
			poolID++

			// We don't increment 'i' here, because we need to re-test the
			// current 'allocated' against the next pool available.
			continue
		}

		// If the pool doesn't overlap and has a binary value lower than the
		// current 'allocated', we found the right spot.
		if p.Prefix.Addr().Less(allocated.Addr()) {
			copy(a.allocated[i+1:], a.allocated[i:])
			a.allocated[i] = netip.PrefixFrom(p.Prefix.Addr(), p.Size)
			return a.allocated[i], nil
		}

		i++
	}

	if poolID >= len(a.pools) {
		return netip.Prefix{}, ErrNoFreePool
	}

	// We reached the end of 'allocated', but not the end of pools. Let's
	// try two more times (once on the current 'p', and once on the next pool
	// if any).
	if partialOverlap {
		p := a.pools[poolID]

		prevAlloc := a.allocated[i-1]
		if next := nextPrefixAfter(prevAlloc, p.Prefix, p.Size); next != (netip.Prefix{}) {
			a.allocated = slices.Insert(a.allocated, i, next)
			return next, nil
		}

		// No luck -- next yielded an invalid prefix. There's not enough
		// space left to use this pool.
		poolID++
	}

	// One last chance. Here we don't increment poolID since the last iteration
	// on 'a.allocated' found either:
	//
	// - A full overlap, and incremented 'poolID'.
	// - A partial overlap, and the previous 'if' incremented 'poolID'.
	// - The current 'poolID' comes after the last 'allocated'.
	//
	// Hence, we're sure 'poolID' has never been subnetted yet.
	if poolID < len(a.pools) {
		p := a.pools[poolID]

		a.allocated = append(a.allocated, netip.PrefixFrom(p.Prefix.Addr(), p.Size))
		return a.allocated[i], nil
	}

	return netip.Prefix{}, ErrNoFreePool
}

func lastAddr(p netip.Prefix) netip.Addr {
	return Add(p.Addr(), 1, uint(32-p.Bits())).Prev()
}

func nextPrefix(p netip.Prefix) netip.Prefix {
	return netip.PrefixFrom(lastAddr(p).Next(), p.Bits())
}

// Add returns ip + (x << shift).
func Add(ip netip.Addr, x uint64, shift uint) netip.Addr {
	a := ip.As4()
	addr := binary.BigEndian.Uint32(a[:])
	addr += uint32(x) << shift
	binary.BigEndian.PutUint32(a[:], addr)
	return netip.AddrFrom4(a)
}
