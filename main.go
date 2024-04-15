package main

import (
	"encoding/binary"
	"errors"
	"fmt"
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

// FirstSubnet returns the first subnet of the pool.
func (p Pool) FirstSubnet() netip.Prefix {
	return netip.PrefixFrom(p.Prefix.Addr(), p.Size)
}

func NewAllocator(pools []Pool) (*Allocator, error) {
	for i, p := range pools {
		if p.Prefix.IsValid() {
			return nil, errors.New("NewAllocator: prefix zero found")
		}

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
	}, nil
}

type DoubleCursor[T any] struct {
	a      []T
	b      []T
	ia, ib int
	cmp    func(a, b T) bool
	lastA  bool
}

func NewDoubleCursor[T any](a, b []T, cmp func(a, b T) bool) *DoubleCursor[T] {
	return &DoubleCursor[T]{
		a:   a,
		b:   b,
		cmp: cmp,
	}
}

func (dc *DoubleCursor[T]) Get() T {
	if dc.ia < len(dc.a) && dc.ib < len(dc.b) {
		if dc.cmp(dc.a[dc.ia], dc.b[dc.ib]) {
			dc.lastA = true
			return dc.a[dc.ia]
		}
		dc.lastA = false
		return dc.b[dc.ib]
	} else if dc.ia < len(dc.a) {
		dc.lastA = true
		return dc.a[dc.ia]
	} else if dc.ib < len(dc.b) {
		dc.lastA = false
		return dc.b[dc.ib]
	}

	return *new(T)
}

func (dc *DoubleCursor[T]) Inc() {
	if dc.lastA {
		dc.ia++
	} else {
		dc.ib++
	}
}

// AllocateNext iterate through its pools of prefixes and allocate the first
// one that doesn't conflict with either existing allocations or 'reserved'. It
// returns ErrNoFreePool if there's no free space. 'reserved' should be sorted.
func (a *Allocator) AllocateNext(reserved []netip.Prefix) (netip.Prefix, error) {
	var poolID int
	var partialOverlap bool
	var prevAlloc netip.Prefix

	dc := NewDoubleCursor(a.allocated, reserved, func(a, b netip.Prefix) bool {
		return a.Addr().Less(b.Addr())
	})

	for {
		allocated := dc.Get()
		if allocated == (netip.Prefix{}) {
			break
		}

		if poolID >= len(a.pools) {
			return netip.Prefix{}, ErrNoFreePool
		}
		p := a.pools[poolID]

		if allocated.Overlaps(p.Prefix) {
			dc.Inc()

			if allocated.Bits() <= p.Prefix.Bits() {
				// The current 'allocated' prefix is bigger than the pool. The
				// pool is fully overlapped.
				prevAlloc = netip.Prefix{}
				partialOverlap = false
				poolID++
				continue
			}

			// If the pool isn't fully overlapped, and no previous 'allocated'
			// was found to partially overlap 'p', we need to test whether
			// there's enough space available at the beginning of 'p'.
			if !partialOverlap && Distance(p.FirstSubnet(), allocated, p.Size) >= 1 {
				// Okay, so there's at least a whole subnet available between
				// the start of 'p' and 'allocated'.
				next := p.FirstSubnet()
				a.allocated = slices.Insert(a.allocated, dc.ia, next)
				return next, nil
			}

			// If the pool 'p' was already found to be partially overlapped, we
			// need to test whether there's enough space between 'prevAlloc'
			// and current 'allocated'. That is, 2 subnets: one for 'prevAlloc'
			// and one for the subnet we want to allocate.
			if partialOverlap && Distance(prevAlloc, allocated, p.Size) >= 2 {
				// Okay, so there's at least a whole subnet available after
				// 'prevAlloc'.
				next := nextPrefixAfter(prevAlloc, p)
				a.allocated = slices.Insert(a.allocated, dc.ia, next)
				return next, nil
			}

			if lastAddr(allocated) == lastAddr(p.Prefix) {
				// The last address of the current 'allocated' prefix is the
				// same as the last address of the pool, it's fully overlapped.
				prevAlloc = netip.Prefix{}
				partialOverlap = false
				poolID++
				continue
			}

			// This pool is partially overlapped. We need to test the next 'allocated'.
			prevAlloc = allocated
			partialOverlap = true
			continue
		}

		// Okay, so previous 'allocated' overlapped and current doesn't. Now
		// the question is: is there enough space left between previous
		// 'allocated' and the end of 'p'?
		if partialOverlap {
			partialOverlap = false

			if next := nextPrefixAfter(prevAlloc, p); next != (netip.Prefix{}) {
				a.allocated = slices.Insert(a.allocated, dc.ia, next)
				return next, nil
			}

			// No luck -- nextPrefixAfter yielded an invalid prefix. There's
			// not enough space left to use this pool.
			poolID++

			// We don't increment 'dc' here, we need to re-test the current
			// 'allocated' against the next pool available.
			continue
		}

		// If the pool doesn't overlap and is sorted before the current
		// 'allocated', we found the right spot.
		if p.Prefix.Addr().Less(allocated.Addr()) {
			copy(a.allocated[dc.ia+1:], a.allocated[dc.ia:])
			a.allocated[dc.ia] = p.FirstSubnet()
			return a.allocated[dc.ia], nil
		}

		dc.Inc()
		prevAlloc = allocated
	}

	if poolID >= len(a.pools) {
		return netip.Prefix{}, ErrNoFreePool
	}

	// We reached the end of 'allocated', but not the end of pools. Let's
	// try two more times (once on the current 'p', and once on the next pool
	// if any).
	if partialOverlap {
		p := a.pools[poolID]

		if next := nextPrefixAfter(prevAlloc, p); next != (netip.Prefix{}) {
			a.allocated = slices.Insert(a.allocated, dc.ia, next)
			return next, nil
		}

		// No luck -- next yielded an invalid prefix. There's not enough
		// space left to use this pool.
		poolID++
	}

	// One last chance -- we didn't drain the pools yet. We'll try every
	// possible prefix from each remaining pool and take the first prefix that
	// doesn't overlap.
	if poolID < len(a.pools) {
		p := a.pools[poolID]

		next := p.FirstSubnet()
		a.allocated = append(a.allocated, next)
		return next, nil
	}

	return netip.Prefix{}, ErrNoFreePool
}

// AllocateStatic checks whether 'prefix' conflicts with any current
// allocations and add it to the allocation list if it doesn't. Otherwise it
// returns an error.
func (a *Allocator) AllocateStatic(prefix netip.Prefix) error {
	if !prefix.IsValid() {
		return fmt.Errorf("AllocateStatic: prefix %s is not valid", prefix)
	}

	for i, allocated := range a.allocated {
		if allocated.Overlaps(prefix) {
			return fmt.Errorf("AllocateStatic: prefix %s overlaps with %s", prefix, allocated)
		}
		if prefix.Addr().Compare(allocated.Addr()) < 0 {
			a.allocated = slices.Insert(a.allocated, i, prefix)
			return nil
		}
	}

	a.allocated = slices.Insert(a.allocated, len(a.allocated), prefix)
	return nil
}

// Deallocate removes 'prefix' from the list of allocations. It returns an
// error if this prefix wasn't allocated.
func (a *Allocator) Deallocate(prefix netip.Prefix) error {
	for i, allocated := range a.allocated {
		if allocated.Addr().Compare(prefix.Addr()) == 0 && allocated.Bits() == prefix.Bits() {
			a.allocated = slices.Delete(a.allocated, i, 1)
			return nil
		}
	}

	return fmt.Errorf("deallocate: %s is not allocated", prefix)
}

func lastAddr(p netip.Prefix) netip.Addr {
	return Add(p.Addr(), 1, uint(p.Addr().BitLen()-p.Bits())).Prev()
}

func nextPrefixAfter(prev netip.Prefix, p Pool) netip.Prefix {
	addr := Add(prev.Addr(), 1, uint(prev.Addr().BitLen()-p.Size))
	prefix := netip.PrefixFrom(addr, p.Size).Masked()

	// If 'prev' is the last prefix from pool 'p', the next 'prefix' won't
	// overlap with the pool.
	if p.Prefix.Overlaps(prefix) {
		return prefix
	}

	return netip.Prefix{}
}

// Add returns ip + (x << shift).
func Add(ip netip.Addr, x uint64, shift uint) netip.Addr {
	a := ip.As4()
	addr := binary.BigEndian.Uint32(a[:])
	addr += uint32(x) << shift
	binary.BigEndian.PutUint32(a[:], addr)
	return netip.AddrFrom4(a)
}

// Distance computes the number of subnets of size 'sz' available between 'p1'
// and 'p2'.
func Distance(p1 netip.Prefix, p2 netip.Prefix, sz int) uint32 {
	p1 = netip.PrefixFrom(p1.Addr(), sz).Masked()
	p2 = netip.PrefixFrom(p2.Addr(), sz).Masked()

	return Substract(p2.Addr(), p1.Addr()) >> (p1.Addr().BitLen() - sz)
}

func Substract(ip1 netip.Addr, ip2 netip.Addr) uint32 {
	a1 := ip1.As4()
	a2 := ip2.As4()
	addr1 := binary.BigEndian.Uint32(a1[:])
	addr2 := binary.BigEndian.Uint32(a2[:])
	return addr1 - addr2
}
