package grpc

import (
	"testing"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// FuzzDecoder drives the decoder the way the wire does: an arbitrary sequence
// of Push and Next calls over arbitrary bytes. The interleaving is the point —
// the buffer bound is only interesting when a chunk arrives before the previous
// message has been drained, and a fuzzer that pushes one well-formed message at
// a time never reaches it.
//
// The script is read as: one control byte giving a chunk length, that many
// payload bytes, then one byte deciding whether to drain with Next or push
// again without draining. Pushing twice with no Next between is exactly what
// Stream.Recv never does and what a bound living in the caller would miss.
func FuzzDecoder(f *testing.F) {
	one, _ := AppendMessage(nil, []byte("hello"))
	two, _ := AppendMessage(append([]byte(nil), one...), []byte("world"))
	f.Add([]byte{5, 0, 0, 0, 0, 3, 1})                       // a bare prefix, drained
	f.Add(append([]byte{byte(len(one))}, append(one, 1)...)) // one whole message
	f.Add(append([]byte{byte(len(two))}, append(two, 1)...)) // two in one chunk
	f.Add([]byte{5, 0, 0xFF, 0xFF, 0xFF, 0xFF, 1})           // 4 GiB length prefix
	f.Add([]byte{6, 1, 0, 0, 0, 1, 'x', 1})                  // compressed flag set
	f.Add([]byte{3, 0, 0, 0, 0, 3, 0, 0, 1})                 // split prefix, no drain between
	f.Add([]byte{0, 1, 0, 0})                                // empty chunks

	f.Fuzz(func(t *testing.T, script []byte) {
		var d decoder
		d.max = 1 << 12 // small, so the bound is reachable inside the fuzz budget
		ceiling := 2*(d.limit()+prefixLen) + 512

		for i := 0; i < len(script); {
			n := int(script[i])
			i++
			if n > len(script)-i {
				n = len(script) - i
			}
			d.Push(script[i : i+n])
			i += n

			drain := i < len(script) && script[i]&1 == 1
			if i < len(script) {
				i++
			}
			if drain {
				for {
					msg, ok, err := d.Next()
					if err != nil || !ok {
						break
					}
					if len(msg) > d.limit() {
						t.Fatalf("Next returned %d bytes, past the %d limit", len(msg), d.limit())
					}
				}
			}

			// Invariants that must hold no matter what the peer sent or in
			// what order the caller drained.
			if d.off < 0 || d.off > len(d.buf) {
				t.Fatalf("offset %d outside buf of %d", d.off, len(d.buf))
			}
			if d.Pending() < 0 {
				t.Fatalf("Pending = %d", d.Pending())
			}
			if !d.overLimit() && len(d.buf) > ceiling {
				t.Fatalf("buffer grew to %d bytes with max=%d, past the %d bound",
					len(d.buf), d.limit(), ceiling)
			}
		}
	})
}

// FuzzResponseFields drives every parser that reads a peer-supplied field
// value. None of them may panic, and none may hand back a status the peer did
// not send.
func FuzzResponseFields(f *testing.F) {
	f.Add("0", "ok", "200", "application/grpc")
	f.Add("4294967295", "%FF%FF", "000200", "application/grpc+proto")
	f.Add("", "%", "", "")
	f.Add("-1", "a%0Ab", "18446744073709551816", "text/html")
	f.Add("16", "%zz%2", "999", "application/grpcXX")

	f.Fuzz(func(t *testing.T, statusV, messageV, httpStatus, contentType string) {
		code := parseStatusCode(statusV)
		// String must not panic for any code the peer can name, including on a
		// 32-bit target where a naive int conversion would go negative.
		_ = code.String()

		msg := decodeMessage(messageV)
		for i := 0; i < len(msg); i++ {
			if controlByte(msg[i]) {
				t.Fatalf("decodeMessage(%q) kept control byte %#x", messageV, msg[i])
			}
		}
		st := Status{Code: code, Message: msg}
		_ = st.Error()

		fields := []conn.HeaderField{
			{Name: []byte(":status"), Value: []byte(httpStatus)},
			{Name: []byte("content-type"), Value: []byte(contentType)},
		}
		n := pseudoStatus(fields)
		if n < 0 || n > 999 {
			t.Fatalf("pseudoStatus(%q) = %d, outside the three-digit grammar", httpStatus, n)
		}
		_ = validContentType(fields)
		_ = cloneFields(fields)
	})
}

// FuzzBinMetadata drives the base64 decode of a "-bin" metadata value, which is
// sized from peer input.
func FuzzBinMetadata(f *testing.F) {
	f.Add("AP8Q")
	f.Add("YQ")
	f.Add("AA==AA==")
	f.Add("!!!!")
	f.Add("")

	f.Fuzz(func(t *testing.T, encoded string) {
		md := []conn.HeaderField{{Name: []byte("k-bin"), Value: []byte(encoded)}}
		v, ok, err := MetadataValue(md, "k-bin")
		if !ok {
			t.Fatal("a present key reported absent")
		}
		if err != nil && v != nil {
			t.Fatalf("value %q returned alongside error %v", v, err)
		}
	})
}
