package client

import (
	"context"
	"errors"
	"strings"

	"github.com/lodgvideon/poseidon-http-client/conn"
)

// This file is the OPT-IN ergonomic layer. Everything here is convenience that
// allocates only when you call it; none of it is on the zero-allocation hot
// path. The expert path is unchanged: build a Request literal, reuse a
// caller-owned Response across Do calls (Response.Reset), and prebuild/reuse a
// []HeaderField slice. Reach for the helpers below for one-off requests,
// scripts, and tests where clarity matters more than the last allocation.

// HeaderField is a request/response header field (name + value bytes). It is a
// type alias for conn.HeaderField, re-exported here so callers do not need to
// import the lower-level conn package for the most common type.
type HeaderField = conn.HeaderField

// H builds a regular request header field. The name is lower-cased, since
// RFC 7540 §8.1.2 requires HTTP/2 field names to be lowercase; the value is
// copied verbatim. Allocates (two byte slices); not for the hot path — there,
// prebuild a []conn.HeaderField once and reuse it.
//
//	req.Headers = []conn.HeaderField{
//	    client.H("accept", "application/json"),
//	    client.H("x-api-key", token),
//	}
func H(name, value string) HeaderField {
	return HeaderField{
		Name:  []byte(strings.ToLower(name)),
		Value: []byte(value),
	}
}

// NewRequest returns a *Request for method and path with WantBody enabled, so
// the obvious path captures the response body instead of silently dropping it.
// Mirrors net/http.NewRequest's shape. The returned Request escapes to the
// heap; in a zero-allocation hot loop, reuse a Request literal instead and set
// WantBody yourself.
func NewRequest(method, path string) *Request {
	return &Request{Method: method, Path: path, WantBody: true}
}

// GET is shorthand for NewRequest("GET", path).
func GET(path string) *Request { return NewRequest("GET", path) }

// POST is shorthand for a body-carrying POST with WantBody enabled. body is
// referenced, not copied; it must stay valid until Do returns.
func POST(path string, body []byte) *Request {
	r := NewRequest("POST", path)
	r.Body = body
	return r
}

// WithHeaders sets r.Headers and returns r for chaining:
//
//	resp := &client.Response{}
//	_ = c.Do(ctx, client.GET("/v1/things").WithHeaders(client.H("accept", "application/json")), resp)
func (r *Request) WithHeaders(h ...HeaderField) *Request {
	r.Headers = h
	return r
}

// Header returns the value of the first response header whose name matches
// (case-insensitively) and whether it was present. Read-only: the returned
// slice aliases Response-owned memory and is valid until the next Reset — copy
// it (or use HeaderString) to retain. Allocation-free.
func (r *Response) Header(name string) ([]byte, bool) {
	for i := range r.Headers {
		if equalFoldASCII(r.Headers[i].Name, name) {
			return r.Headers[i].Value, true
		}
	}
	return nil, false
}

// HeaderString is Header returning a freshly-allocated string copy, safe to
// retain past the next Reset.
func (r *Response) HeaderString(name string) (string, bool) {
	if v, ok := r.Header(name); ok {
		return string(v), true
	}
	return "", false
}

// CopyBody returns a heap copy of the response body, safe to retain after the
// next Reset(). nil when the body is empty.
func (r *Response) CopyBody() []byte {
	if len(r.Body) == 0 {
		return nil
	}
	return append([]byte(nil), r.Body...)
}

// Clone returns a detached deep copy of the response (Status, Headers, Body,
// Trailers, BytesReceived) backed by its own memory, safe to retain after the
// source Response is Reset or reused. The streaming BodyReader and pooled slabs
// are not carried over.
func (r *Response) Clone() *Response {
	return &Response{
		Status:        r.Status,
		Headers:       copyHeaders(r.Headers),
		Body:          r.CopyBody(),
		Trailers:      copyHeaders(r.Trailers),
		BytesReceived: r.BytesReceived,
	}
}

// DataCopy returns a heap copy of the event's DATA payload, safe to retain past
// the next Recv/Close (which recycle the underlying pooled buffer). nil for
// non-data events or empty payloads.
func (e StreamEvent) DataCopy() []byte {
	if len(e.Data) == 0 {
		return nil
	}
	return append([]byte(nil), e.Data...)
}

// Stream issues a streaming request and invokes fn for each StreamEvent until
// the stream ends, fn returns an error, or ctx is cancelled. It always closes
// the underlying StreamResponse, so callers cannot leak the pooled connection
// slot by forgetting Close — the most common DoStream footgun.
//
// fn must not retain StreamEvent.Data past its return (the buffer is recycled
// on the next event); use StreamEvent.DataCopy to keep bytes.
//
//	err := c.Stream(ctx, client.GET("/events"), func(ev client.StreamEvent) error {
//	    if ev.Type == client.EventData {
//	        process(ev.Data)
//	    }
//	    return nil
//	})
func (c *Client) Stream(ctx context.Context, req *Request, fn func(StreamEvent) error) error {
	var sr StreamResponse
	if err := c.DoStream(ctx, req, &sr); err != nil {
		return err
	}
	defer func() { _ = sr.Close() }()
	for {
		ev, err := sr.Recv(ctx)
		if errors.Is(err, ErrStreamEnded) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := fn(ev); err != nil {
			return err
		}
	}
}

// equalFoldASCII reports whether b equals s under ASCII case folding, without
// allocating. HTTP/2 field names are ASCII (RFC 7540 §8.1.2).
func equalFoldASCII(b []byte, s string) bool {
	if len(b) != len(s) {
		return false
	}
	for i := 0; i < len(b); i++ {
		if lowerASCII(b[i]) != lowerASCII(s[i]) {
			return false
		}
	}
	return true
}

func lowerASCII(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}
