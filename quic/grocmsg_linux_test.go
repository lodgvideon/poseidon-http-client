//go:build linux

package quic

import (
	"bytes"
	"encoding/binary"
	"syscall"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// appendCmsg appends one control message {level, typ} carrying body, laid out
// exactly as the kernel would, and returns the extended buffer.
func appendCmsg(oob []byte, level, typ int32, body []byte) []byte {
	start := len(oob)
	oob = append(oob, make([]byte, syscall.CmsgSpace(len(body)))...)
	h := (*syscall.Cmsghdr)(unsafe.Pointer(&oob[start]))
	h.Level = level
	h.Type = typ
	h.SetLen(syscall.CmsgLen(len(body)))
	copy(oob[start+syscall.CmsgLen(0):], body)
	return oob
}

// groCmsg is one UDP_GRO control message reporting segSize as a host-order u16,
// which is what the kernel attaches.
func groCmsg(segSize uint16) []byte {
	var body [2]byte
	binary.NativeEndian.PutUint16(body[:], segSize)
	return appendCmsg(nil, solUDP, udpGRO, body[:])
}

// TestParseGROSegmentSize_MatchesStdlib is the differential test. The hand-walk
// replaced syscall.ParseSocketControlMessage to avoid its per-call slice
// allocation, so it has to agree with it on every buffer the stdlib can parse —
// including the ones where UDP_GRO is not the first message, or not present.
func TestParseGROSegmentSize_MatchesStdlib(t *testing.T) {
	u32 := func(v uint32) []byte {
		var b [4]byte
		binary.NativeEndian.PutUint32(b[:], v)
		return b[:]
	}
	u16 := func(v uint16) []byte {
		var b [2]byte
		binary.NativeEndian.PutUint16(b[:], v)
		return b[:]
	}

	cases := []struct {
		name string
		oob  []byte
	}{
		{"empty", nil},
		{"gro u16", groCmsg(1200)},
		{"gro u32", appendCmsg(nil, solUDP, udpGRO, u32(1400))},
		{"gro zero", groCmsg(0)},
		{"gro max u16", groCmsg(65535)},
		{"other level only", appendCmsg(nil, syscall.SOL_SOCKET, syscall.SO_RCVBUF, u32(7))},
		{"other type at udp level", appendCmsg(nil, solUDP, udpSegment, u16(900))},
		{"gro after an unrelated cmsg", appendCmsg(appendCmsg(nil, syscall.SOL_SOCKET, syscall.SO_RCVBUF, u32(7)), solUDP, udpGRO, u16(1100))},
		{"gro before an unrelated cmsg", appendCmsg(appendCmsg(nil, solUDP, udpGRO, u16(1300)), syscall.SOL_SOCKET, syscall.SO_RCVBUF, u32(7))},
		{"gro with an empty body", appendCmsg(nil, solUDP, udpGRO, nil)},
		{"gro with a one-byte body", appendCmsg(nil, solUDP, udpGRO, []byte{9})},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			want := stdlibGROSegmentSize(t, c.oob)

			got := parseGROSegmentSize(c.oob)

			assert.Equalf(t, want, got,
				"parseGROSegmentSize = %d, stdlib walk = %d — the hand-walk replaced "+
					"syscall.ParseSocketControlMessage and must agree with it", got, want)
		})
	}
}

// stdlibGROSegmentSize is the implementation this replaced, kept in the test as
// the oracle the hand-walk is diffed against.
func stdlibGROSegmentSize(t *testing.T, oob []byte) int {
	t.Helper()
	msgs, err := syscall.ParseSocketControlMessage(oob)
	if err != nil {
		return 0
	}
	for _, m := range msgs {
		if m.Header.Level != solUDP || m.Header.Type != udpGRO {
			continue
		}
		switch {
		case len(m.Data) >= 4:
			return int(binary.NativeEndian.Uint32(m.Data))
		case len(m.Data) >= 2:
			return int(binary.NativeEndian.Uint16(m.Data))
		}
	}
	return 0
}

// TestParseGROSegmentSize_MalformedYieldsZero pins that a control buffer the
// kernel would never produce cannot panic or spin. The parse reads a length out
// of memory the kernel filled, so every one of these is a bounds check that has
// to hold rather than a hypothetical.
func TestParseGROSegmentSize_MalformedYieldsZero(t *testing.T) {
	full := groCmsg(1200)
	hdrLen := syscall.CmsgLen(0)

	zeroLen := append([]byte(nil), full...)
	(*syscall.Cmsghdr)(unsafe.Pointer(&zeroLen[0])).Len = 0

	hugeLen := append([]byte(nil), full...)
	(*syscall.Cmsghdr)(unsafe.Pointer(&hugeLen[0])).SetLen(1 << 30)

	shortLen := append([]byte(nil), full...)
	(*syscall.Cmsghdr)(unsafe.Pointer(&shortLen[0])).SetLen(hdrLen - 1)

	cases := []struct {
		name string
		oob  []byte
	}{
		{"shorter than a header", full[:hdrLen-1]},
		{"header truncated mid-body", full[:hdrLen+1]},
		{"Len of zero", zeroLen},
		{"Len past the buffer", hugeLen},
		{"Len below the header size", shortLen},
		{"all ones", bytes.Repeat([]byte{0xff}, len(full))},
		{"all zeroes", make([]byte, len(full))},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseGROSegmentSize(c.oob)

			assert.Zerof(t, got,
				"parseGROSegmentSize = %d on a malformed buffer, want 0 — the parse reads a "+
					"length out of kernel-filled memory, so every bounds check has to hold", got)
		})
	}
}

// TestParseGROSegmentSize_DoesNotAllocate is the point of the rewrite:
// syscall.ParseSocketControlMessage appends every message to a slice, so a
// coalesced recvmsg — the read UDP_GRO exists to produce — paid one heap
// allocation just to learn the segment size. Measured at 1.00 before, 0 after.
func TestParseGROSegmentSize_DoesNotAllocate(t *testing.T) {
	oob := groCmsg(1200)
	// testify never runs inside the measured closure below: it reflects and
	// allocates, and AllocsPerRun counts the whole process.
	require.Equal(t, 1200, parseGROSegmentSize(oob), "setup: the fixture must parse to 1200")

	n := testing.AllocsPerRun(500, func() { sinkSegSize = parseGROSegmentSize(oob) })

	assert.Zerof(t, n,
		"parseGROSegmentSize allocates %.2f times per coalesced recvmsg, want 0", n)
}

// sinkSegSize keeps the measured call from being optimised away.
var sinkSegSize int

// FuzzParseGROSegmentSize drives the parse with arbitrary bytes. It parses a
// length field the kernel wrote and indexes on it, so the property under test is
// simply that no input reaches a panic — and that whatever it returns, the
// stdlib walk agrees.
func FuzzParseGROSegmentSize(f *testing.F) {
	f.Add(groCmsg(1200))
	f.Add(appendCmsg(nil, syscall.SOL_SOCKET, syscall.SO_RCVBUF, []byte{1, 2, 3, 4}))
	f.Add([]byte{})
	f.Add(bytes.Repeat([]byte{0xff}, 64))
	f.Fuzz(func(t *testing.T, oob []byte) {
		got := parseGROSegmentSize(oob)
		// The stdlib is the oracle wherever it can parse the buffer at all.
		if msgs, err := syscall.ParseSocketControlMessage(oob); err == nil {
			want := 0
			for _, m := range msgs {
				if m.Header.Level != solUDP || m.Header.Type != udpGRO {
					continue
				}
				switch {
				case len(m.Data) >= 4:
					want = int(binary.NativeEndian.Uint32(m.Data))
				case len(m.Data) >= 2:
					want = int(binary.NativeEndian.Uint16(m.Data))
				}
				break
			}
			if got != want {
				t.Fatalf("parseGROSegmentSize = %d, stdlib = %d, oob = %x", got, want, oob)
			}
		}
	})
}
