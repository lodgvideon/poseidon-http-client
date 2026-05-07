package client

import (
	"errors"
	"testing"

	"github.com/lodgvideon/poseidon-http-client/hpack"
)

func TestValidateRequest_OK(t *testing.T) {
	req := &Request{Method: "GET", Path: "/"}
	if err := validateRequest(req); err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
}

func TestValidateRequest_NoMethod(t *testing.T) {
	req := &Request{Path: "/"}
	err := validateRequest(req)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_NoPath(t *testing.T) {
	req := &Request{Method: "GET"}
	err := validateRequest(req)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_PseudoHeaderInRegular(t *testing.T) {
	req := &Request{
		Method: "GET", Path: "/",
		Headers: []hpack.HeaderField{
			{Name: []byte(":authority"), Value: []byte("example.com")},
		},
	}
	err := validateRequest(req)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestValidateRequest_NilRequest(t *testing.T) {
	err := validateRequest(nil)
	if !errors.Is(err, ErrInvalidRequest) {
		t.Fatalf("expected ErrInvalidRequest, got %v", err)
	}
}

func TestParseStatus_Found(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("200")},
		{Name: []byte("content-type"), Value: []byte("application/json")},
	}
	st, rest, err := parseStatus(in)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if st != 200 {
		t.Fatalf("status = %d, want 200", st)
	}
	if len(rest) != 1 || string(rest[0].Name) != "content-type" {
		t.Fatalf("regular headers wrong: %+v", rest)
	}
}

func TestParseStatus_Missing(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte("content-type"), Value: []byte("application/json")},
	}
	_, _, err := parseStatus(in)
	if !errors.Is(err, ErrEmptyResponse) {
		t.Fatalf("expected ErrEmptyResponse, got %v", err)
	}
}

func TestParseStatus_NotNumeric(t *testing.T) {
	in := []hpack.HeaderField{
		{Name: []byte(":status"), Value: []byte("OK")},
	}
	_, _, err := parseStatus(in)
	if !errors.Is(err, ErrEmptyResponse) {
		t.Fatalf("expected ErrEmptyResponse, got %v", err)
	}
}
