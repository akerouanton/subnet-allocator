package main

import (
	"net/netip"
	"testing"

	"gotest.tools/v3/assert"
)

func TestAllocate(t *testing.T) {
	testcases := map[string]*struct {
		allocator *Allocator
		reserved  []netip.Prefix
		expPrefix netip.Prefix
		expErr    error
	}{
		"First allocated overlaps at the end of first pool": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					// Partial overlap with enough space remaining
					netip.MustParsePrefix("192.168.255.0/24"),
				},
			},
			expPrefix: netip.MustParsePrefix("192.168.0.0/24"),
		},
		"First pool fully overlapped, next overlapped in the middle": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("10.20.0.0/16"), Size: 24},
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					netip.MustParsePrefix("10.0.0.0/8"),
					// Partial overlap with enough space remaining
					netip.MustParsePrefix("192.168.128.0/24"),
				},
			},
			expPrefix: netip.MustParsePrefix("192.168.0.0/24"),
		},
		"First pool fully overlapped, next overlapped at the beginning and in the middle": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("10.20.0.0/16"), Size: 24},
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					netip.MustParsePrefix("10.0.0.0/8"),
					// Partial overlap with enough space remaining
					netip.MustParsePrefix("192.168.0.0/24"),
					netip.MustParsePrefix("192.168.128.0/24"),
				},
			},
			expPrefix: netip.MustParsePrefix("192.168.1.0/24"),
		},
		"Partial overlap at the end, enough space": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					// Partial overlap with enough space remaining
					netip.MustParsePrefix("192.168.0.0/24"),
					netip.MustParsePrefix("192.168.1.0/24"),
					netip.MustParsePrefix("192.168.2.3/30"),
				},
			},
			expPrefix: netip.MustParsePrefix("192.168.3.0/24"),
		},
		"Partial overlap at the end of allocated and reserved, enough space": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					// Partial overlap with enough space remaining
					netip.MustParsePrefix("192.168.0.0/24"),
					netip.MustParsePrefix("192.168.1.0/24"),
					netip.MustParsePrefix("192.168.2.3/30"),
				},
			},
			reserved: []netip.Prefix{
				netip.MustParsePrefix("192.168.2.4/30"),
				netip.MustParsePrefix("192.168.3.0/30"),
			},
			expPrefix: netip.MustParsePrefix("192.168.4.0/24"),
		},
		"Partial overlap, same prefix, enough space": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					// Partial overlap with enough space remaining
					netip.MustParsePrefix("192.168.0.0/24"),
				},
			},
			reserved: []netip.Prefix{
				netip.MustParsePrefix("192.168.0.0/24"),
			},
			expPrefix: netip.MustParsePrefix("192.168.1.0/24"),
		},
		"Partial overlap in the middle, enough space": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("172.16.0.0/15"), Size: 16},
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					netip.MustParsePrefix("172.16.0.0/16"),
					// Partial overlap with enough space remaining
					netip.MustParsePrefix("192.168.0.0/24"),
				},
			},
			expPrefix: netip.MustParsePrefix("172.17.0.0/16"),
		},
		"Partial overlap in the middle, not enough space left": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("172.16.0.0/15"), Size: 16},
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					netip.MustParsePrefix("172.16.0.0/16"),
					netip.MustParsePrefix("172.17.0.0/16"),
					// Partial overlap with enough space remaining
					netip.MustParsePrefix("192.168.0.0/24"),
				},
			},
			expPrefix: netip.MustParsePrefix("192.168.1.0/24"),
		},
		"Partial overlap at the start, enough space left in the middle": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("30.0.0.0/31"), Size: 31},
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					// Partial overlap but not enough space left
					netip.MustParsePrefix("30.0.0.0/32"),
					netip.MustParsePrefix("200.0.0.0/8"),
				},
			},
			expPrefix: netip.MustParsePrefix("192.168.0.0/24"),
		},
		"Full overlap with small allocations, enough space": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("40.0.0.0/31"), Size: 31},
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					// Fully overlap with smaller allocations
					netip.MustParsePrefix("40.0.0.0/32"),
					netip.MustParsePrefix("40.0.0.1/32"),
				},
			},
			expPrefix: netip.MustParsePrefix("192.168.0.0/24"),
		},
		"Full overlap with same size allocation": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("50.0.0.0/31"), Size: 31},
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					// Fully overlap with same-size allocation
					netip.MustParsePrefix("50.0.0.0/31"),
				},
			},
			expPrefix: netip.MustParsePrefix("192.168.0.0/24"),
		},
		"Full overlap with bigger allocation": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("172.16.0.0/12"), Size: 24},
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					// Fully overlap with bigger allocation
					netip.MustParsePrefix("172.0.0.0/8"),
				},
			},
			expPrefix: netip.MustParsePrefix("192.168.0.0/24"),
		},
		"Extra allocations, no pool left": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("172.16.0.0/15"), Size: 16},
				},
				allocated: []netip.Prefix{
					netip.MustParsePrefix("172.16.0.0/16"),
					netip.MustParsePrefix("172.17.0.0/16"),
					netip.MustParsePrefix("192.168.0.0/24"),
				},
			},
			expErr: ErrNoFreePool,
		},
		"Pools fully allocated": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("172.16.0.0/15"), Size: 16},
					{Prefix: netip.MustParsePrefix("192.168.0.0/23"), Size: 24},
				},
				allocated: []netip.Prefix{
					netip.MustParsePrefix("172.16.0.0/16"),
					netip.MustParsePrefix("172.17.0.0/16"),
					netip.MustParsePrefix("192.168.0.0/24"),
					netip.MustParsePrefix("192.168.1.0/24"),
				},
			},
			expErr: ErrNoFreePool,
		},
		"Partial overlap, not enough space left": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("172.16.0.0/15"), Size: 16},
					{Prefix: netip.MustParsePrefix("192.168.0.0/23"), Size: 24},
				},
				allocated: []netip.Prefix{
					netip.MustParsePrefix("172.16.0.0/16"),
					netip.MustParsePrefix("172.17.0.0/16"),
					netip.MustParsePrefix("192.168.0.0/24"),
					netip.MustParsePrefix("192.168.1.1/31"),
				},
			},
			expErr: ErrNoFreePool,
		},
		"Minimal overlap at the start, enough space": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("172.16.0.0/15"), Size: 16},
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					netip.MustParsePrefix("172.16.0.0/16"),
					netip.MustParsePrefix("192.168.1.1/31"),
				},
			},
			expPrefix: netip.MustParsePrefix("172.17.0.0/16"),
		},
	}

	for tcname := range testcases {
		tc := testcases[tcname]
		t.Run(tcname, func(t *testing.T) {
			p, err := tc.allocator.AllocateNext(tc.reserved)

			assert.ErrorIs(t, err, tc.expErr)
			assert.Equal(t, p, tc.expPrefix)
		})
	}
}

func TestAllocateStatic(t *testing.T) {
	a := &Allocator{
		pools: []Pool{},
		allocated: []netip.Prefix{
			netip.MustParsePrefix("172.16.0.0/16"),
			netip.MustParsePrefix("192.168.0.0/24"),
		},
	}

	// Insert in the middle
	assert.NilError(t, a.AllocateStatic(netip.MustParsePrefix("172.17.0.0/16")))
	// Insert at the end
	assert.NilError(t, a.AllocateStatic(netip.MustParsePrefix("192.168.1.0/24")))

	// Small prefix overlaps with a bigger allocated
	assert.ErrorContains(t, a.AllocateStatic(netip.MustParsePrefix("192.168.0.0/31")), "AllocateStatic: prefix 192.168.0.0/31 overlaps with 192.168.0.0/24")
	// Big prefixes overlap with a smaller allocated
	assert.ErrorContains(t, a.AllocateStatic(netip.MustParsePrefix("172.16.0.0/12")), "AllocateStatic: prefix 172.16.0.0/12 overlaps with 172.16.0.0/16")
	assert.ErrorContains(t, a.AllocateStatic(netip.MustParsePrefix("172.0.0.0/8")), "AllocateStatic: prefix 172.0.0.0/8 overlaps with 172.16.0.0/16")
}

func TestDeallocate(t *testing.T) {
	a := &Allocator{
		pools: []Pool{},
		allocated: []netip.Prefix{
			netip.MustParsePrefix("172.16.0.0/16"),
			netip.MustParsePrefix("192.168.0.0/24"),
		},
	}

	assert.NilError(t, a.Deallocate(netip.MustParsePrefix("172.16.0.0/16")))
	assert.Equal(t, len(a.allocated), 1)

	assert.ErrorContains(t, a.Deallocate(netip.MustParsePrefix("172.16.0.0/16")), "deallocate: 172.16.0.0/16 is not allocated")
	assert.Equal(t, len(a.allocated), 1)
}

func BenchmarkAllocate(b *testing.B) {
	a := &Allocator{
		pools: []Pool{
			{Prefix: netip.MustParsePrefix("30.0.0.0/31"), Size: 31},
			{Prefix: netip.MustParsePrefix("40.0.0.0/31"), Size: 31},
			{Prefix: netip.MustParsePrefix("50.0.0.0/31"), Size: 31},
			{Prefix: netip.MustParsePrefix("172.16.0.0/12"), Size: 24},
			{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
		},
		allocated: []netip.Prefix{
			// Partial overlap but not enough space remaining
			netip.MustParsePrefix("30.0.0.0/32"),
			// Fully overlap with smaller allocations
			netip.MustParsePrefix("40.0.0.0/32"),
			netip.MustParsePrefix("40.0.0.1/32"),
			// Fully overlap with same-size allocation
			netip.MustParsePrefix("50.0.0.0/31"),
			// Fully overlap with bigger allocation
			netip.MustParsePrefix("172.0.0.0/8"),
			// Partial overlap with enough space remaining
			netip.MustParsePrefix("192.168.0.0/24"),
			netip.MustParsePrefix("192.168.1.0/24"),
			netip.MustParsePrefix("192.168.2.3/30"),
		},
	}

	p, err := a.AllocateNext([]netip.Prefix{})

	assert.NilError(b, err)
	assert.Equal(b, p, netip.MustParsePrefix("192.168.3.0/24"))

	b.Logf("Prefix allocated: %s", p)
}

func BenchmarkEmpty(b *testing.B) {
	a := &Allocator{
		pools: []Pool{
			{Prefix: netip.MustParsePrefix("30.0.0.0/31"), Size: 31},
		},
		allocated: []netip.Prefix{},
	}

	p, err := a.AllocateNext([]netip.Prefix{})

	assert.NilError(b, err)
	assert.Equal(b, p, netip.MustParsePrefix("30.0.0.0/31"))

	b.Logf("Prefix allocated: %s", p)
}

func BenchmarkSerial(b *testing.B) {
	a := &Allocator{
		pools: []Pool{
			{Prefix: netip.MustParsePrefix("10.0.0.0/8"), Size: 24},
		},
		allocated: []netip.Prefix{},
	}

	// 10,000 -> 600ms
	//  1,000 -> 13ms
	//    100 -> 200us
	//     10 -> 14us
	//      1 -> 10us
	imax := 10000
	for i := 0; i < imax; i++ {
		_, err := a.AllocateNext([]netip.Prefix{})
		if err != nil {
			panic(err)
		}
	}

	assert.Equal(b, len(a.allocated), imax)
}

func BenchmarkSerialWithConflict(b *testing.B) {
	prefix := netip.MustParsePrefix("10.0.0.0/8")
	a := &Allocator{
		pools: []Pool{
			{Prefix: prefix, Size: 24},
		},
		allocated: []netip.Prefix{},
	}

	reserved := []netip.Prefix{netip.PrefixFrom(prefix.Addr(), 24)}

	// 5,000 -> 500ms
	// 1,000 -> 17ms
	//   500 -> 4ms
	//    100 -> 246us
	//     10 -> 20us
	//      1 -> 15us
	imax := 1
	for i := 0; i < imax; i++ {
		_, err := a.AllocateNext(reserved)
		assert.NilError(b, err)

		newReservedAddr := Add(reserved[len(reserved)-1].Addr(), 1, 8)
		reserved = append(reserved, netip.PrefixFrom(newReservedAddr, 24))
	}
}
