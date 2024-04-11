package main

import (
	"net/netip"
	"testing"

	"gotest.tools/v3/assert"
)

func TestAllocate(t *testing.T) {
	testcases := map[string]*struct {
		allocator *Allocator
		expPrefix netip.Prefix
		expErr    error
	}{
		"Last partial overlap": {
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
		"Partial overlap in the middle with enough space left": {
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
		"Partial overlap in the middle but with not enough space left": {
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
		"Partial overlap but not enough space left": {
			allocator: &Allocator{
				pools: []Pool{
					{Prefix: netip.MustParsePrefix("30.0.0.0/31"), Size: 31},
					{Prefix: netip.MustParsePrefix("192.168.0.0/16"), Size: 24},
				},
				allocated: []netip.Prefix{
					// Partial overlap but not enough space left
					netip.MustParsePrefix("30.0.0.0/32"),
				},
			},
			expPrefix: netip.MustParsePrefix("192.168.0.0/24"),
		},
		"Full overlap with small allocations": {
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
	}

	for tcname := range testcases {
		tc := testcases[tcname]
		t.Run(tcname, func(t *testing.T) {
			p, err := tc.allocator.Allocate()

			assert.ErrorIs(t, err, tc.expErr)
			assert.Equal(t, p, tc.expPrefix)
		})
	}
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

	p, err := a.Allocate()

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

	p, err := a.Allocate()

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
		_, err := a.Allocate()
		if err != nil {
			panic(err)
		}
	}

	assert.Equal(b, len(a.allocated), imax)
}
