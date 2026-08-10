// Command grpc-example issues a unary gRPC call and then a bidirectional
// streaming call over the same connection, using the poseidon grpc package.
//
// Messages cross the API as []byte: the package carries no protobuf
// dependency, so the caller marshals. Here the payload is a hand-rolled
// protobuf encoding of a single string field, which is what a
// helloworld.Greeter/SayHello request looks like on the wire.
//
//	go run ./examples/grpc -addr localhost:50051 -name world
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/lodgvideon/poseidon-http-client/conn"
	"github.com/lodgvideon/poseidon-http-client/grpc"
)

func main() {
	addr := flag.String("addr", "localhost:50051", "gRPC server host:port")
	name := flag.String("name", "world", "name to greet")
	insecure := flag.Bool("insecure", false, "skip TLS certificate verification")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cc, err := grpc.Dial(ctx, *addr, grpc.Options{
		Conn: conn.ConnOptions{
			Dialer: &conn.TLSDialer{Config: &tls.Config{
				NextProtos:         []string{"h2"},
				InsecureSkipVerify: *insecure,
			}},
			// gRPC servers enforce a minimum ping interval — grpc-go's default
			// is 5 minutes, and it answers anything faster with
			// GOAWAY(ENHANCE_YOUR_CALM) after two strikes. They also refuse
			// pings on a connection with no active stream by default. Leave
			// this at zero unless the server is configured to permit more.
			KeepaliveInterval: 10 * time.Minute,
		},
	})
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer func() { _ = cc.Close() }()

	if err := unary(ctx, cc, *name); err != nil {
		log.Fatalf("unary: %v", err)
	}
	if err := bidi(ctx, cc); err != nil {
		log.Fatalf("bidi: %v", err)
	}
}

// unary performs a single request/response call.
func unary(ctx context.Context, cc *grpc.ClientConn, name string) error {
	md, err := grpc.AppendMetadata(nil, "x-request-id", []byte("example-1"))
	if err != nil {
		return err
	}
	resp, err := cc.Invoke(ctx, "/helloworld.Greeter/SayHello", protoString(1, name), md)
	if err != nil {
		var st *grpc.Status
		if errors.As(err, &st) {
			return fmt.Errorf("rpc failed with %v: %s", st.Code, st.Message)
		}
		return err
	}
	fmt.Printf("SayHello: %d bytes back\n", len(resp))
	return nil
}

// bidi sends and receives concurrently on one call, which is what the send and
// receive halves of a Stream being independently usable buys you.
func bidi(ctx context.Context, cc *grpc.ClientConn) error {
	s, err := cc.NewStream(ctx, "/helloworld.Greeter/SayHelloBidiStream", nil)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	sendErr := make(chan error, 1)
	go func() {
		for i := 0; i < 5; i++ {
			if err := s.Send(ctx, protoString(1, fmt.Sprintf("msg-%d", i))); err != nil {
				sendErr <- err
				return
			}
		}
		sendErr <- s.CloseSend(ctx)
	}()

	n := 0
	for {
		msg, err := s.Recv(ctx)
		if errors.Is(err, io.EOF) {
			break // the call completed with grpc-status OK
		}
		if err != nil {
			return err
		}
		n++
		_ = msg
	}
	if err := <-sendErr; err != nil {
		return err
	}
	fmt.Printf("bidi: %d replies, status %v\n", n, s.Status().Code)
	return nil
}

// protoString encodes one length-delimited protobuf string field. It is the
// smallest possible stand-in for a generated marshaler — enough to talk to the
// canonical helloworld service without a protobuf dependency.
func protoString(fieldNum int, v string) []byte {
	out := make([]byte, 0, len(v)+8)
	out = appendVarint(out, uint64(fieldNum)<<3|2) // wire type 2: length-delimited
	out = appendVarint(out, uint64(len(v)))
	return append(out, v...)
}

// appendVarint appends a protobuf base-128 varint.
func appendVarint(dst []byte, v uint64) []byte {
	for v >= 0x80 {
		dst = append(dst, byte(v)|0x80)
		v >>= 7
	}
	return append(dst, byte(v))
}
