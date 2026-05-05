package hpack

import (
	"bytes"
	"errors"
	"testing"
)

func TestEncodeStringLiteral_Plain(t *testing.T) {
	dst := encodeStringLiteral(nil, []byte("/sample/path"), false)
	wantPrefix := byte(0x0c) // length 12, H=0
	if dst[0] != wantPrefix {
		t.Fatalf("prefix = %#x, want %#x", dst[0], wantPrefix)
	}
	if !bytes.Equal(dst[1:], []byte("/sample/path")) {
		t.Fatalf("body = %q, want %q", dst[1:], "/sample/path")
	}
}

func TestEncodeStringLiteral_Huffman(t *testing.T) {
	dst := encodeStringLiteral(nil, []byte("no-cache"), true)
	if dst[0]&0x80 == 0 {
		t.Fatalf("H bit not set: prefix = %#x", dst[0])
	}
	if dst[0] != 0x86 {
		t.Fatalf("prefix = %#x, want 0x86", dst[0])
	}
}

func TestDecodeStringLiteral_Plain(t *testing.T) {
	src := append([]byte{0x0c}, []byte("/sample/path")...)
	dst := make([]byte, 0, 32)
	out, consumed, err := decodeStringLiteral(dst, src)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(out) != "/sample/path" || consumed != 13 {
		t.Fatalf("got (%q, %d), want (/sample/path, 13)", out, consumed)
	}
}

func TestDecodeStringLiteral_Huffman(t *testing.T) {
	src := []byte{0x86, 0xa8, 0xeb, 0x10, 0x64, 0x9c, 0xbf}
	out, consumed, err := decodeStringLiteral(nil, src)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if string(out) != "no-cache" || consumed != len(src) {
		t.Fatalf("got (%q, %d), want (no-cache, %d)", out, consumed, len(src))
	}
}

func TestDecodeStringLiteral_Truncated(t *testing.T) {
	src := []byte{0x05, 0x61, 0x62, 0x63}
	_, _, err := decodeStringLiteral(nil, src)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("err = %v, want ErrTruncated", err)
	}
}
