package conn

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/lodgvideon/poseidon-http-client/frame"
	"github.com/lodgvideon/poseidon-http-client/hpack"
)

// FuzzConnReader feeds arbitrary bytes as the server side of net.Pipe
// after a successful handshake, asserting our reader goroutine never
// panics. Either the connection terminates cleanly or it errors out.
func FuzzConnReader(f *testing.F) {
	// Seed corpus: a single valid HEADERS frame.
	enc := hpack.NewEncoder()
	block := enc.EncodeBlock(nil, []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("200")},
	})
	hdr := make([]byte, 9)
	hdr[0] = byte(len(block) >> 16)
	hdr[1] = byte(len(block) >> 8)
	hdr[2] = byte(len(block))
	hdr[3] = byte(frame.FrameHeaders)
	hdr[4] = byte(frame.FlagHeadersEndHeaders | frame.FlagHeadersEndStream)
	hdr[5] = 0
	hdr[6] = 0
	hdr[7] = 0
	hdr[8] = 1
	f.Add(append(hdr, block...))

	f.Fuzz(func(_ *testing.T, blob []byte) {
		cli, srv := net.Pipe()
		defer cli.Close()
		defer srv.Close()

		go func() {
			preface := make([]byte, 24)
			_, _ = readN(srv, preface)
			srvFr := frame.NewFramer(srv, srv)
			writeDone := make(chan error, 1)
			go func() { writeDone <- srvFr.WriteSettings(frame.SettingsParams{}) }()
			_, _ = srvFr.ReadFrame(context.Background(), &nilHandler{})
			<-writeDone
			go func() { writeDone <- srvFr.WriteSettingsAck() }()
			_, _ = srvFr.ReadFrame(context.Background(), &nilHandler{})
			<-writeDone
			_, _ = srv.Write(blob) // adversarial bytes
		}()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		c, err := NewClientConn(ctx, cli, ConnOptions{}.defaulted())
		if err != nil {
			return // handshake failed — that's fine
		}
		// Open one stream so the reader has somewhere to push.
		_, _ = c.NewStream(ctx)
		// Sleep briefly to let the reader process bytes.
		time.Sleep(50 * time.Millisecond)
		_ = c.Close()
	})
}
