// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package dns

import (
	"fmt"
	"net"
	"net/netip"
	"testing"
)

func TestIPBehavior(t *testing.T) {
	cases := []struct {
		label string
		ip    net.IP
	}{
		{"pure IPv4 (4-byte)", net.ParseIP("192.168.1.1").To4()},
		{"pure IPv4 (16-byte via ParseIP)", net.ParseIP("192.168.1.1")}, // ParseIP always returns 16-byte
		{"IPv4-mapped IPv6 (::ffff:192.168.1.1)", net.ParseIP("::ffff:192.168.1.1")},
		{"pure IPv6 (2001:db8::1)", net.ParseIP("2001:db8::1")},
	}

	for _, c := range cases {
		t.Run(c.label, func(t *testing.T) {
			ip := c.ip
			fmt.Printf("\n--- %s ---\n", c.label)
			fmt.Printf("  len(ip)         = %d  (IPv4len=4, IPv6len=16)\n", len(ip))
			fmt.Printf("  ip.To4()        = %v  (nil means NOT IPv4 or IPv4-mapped)\n", ip.To4())
			fmt.Printf("  ip.To16()       = %v  (always returns 16-byte; never nil for valid IP)\n", ip.To16())

			a, ok := netip.AddrFromSlice(ip)
			if ok {
				fmt.Printf("  netip.Is4()     = %v\n", a.Is4())
				fmt.Printf("  netip.Is6()     = %v  (true for pure IPv6 AND IPv4-mapped!)\n", a.Is6())
				fmt.Printf("  netip.Is4In6()  = %v  (true only for ::ffff:x.x.x.x)\n", a.Is4In6())
				fmt.Printf("  Unmap().Is4()   = %v  (strips IPv4-in-IPv6 mapping first)\n", a.Unmap().Is4())
			}
		})
	}
}
