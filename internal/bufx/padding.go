package bufx

import "errors"

// errInvalidPadding is returned when the declared pad length exceeds the
// remaining payload (RFC 7540 §6.1: PROTOCOL_ERROR).
//
// Unexported, and deliberately: it was exported and nothing outside this package
// ever referenced the name. That looks like a dead export and is not one — every
// frame.dispatch* call site wraps it with padErr, which keeps it in the error
// chain as the cause under frame.ErrInvalidPadding. What callers discriminate on
// is the frame-level sentinel, which is the right layer for them to match. So
// the name has no consumer while the value does, and unexporting is the change
// that says so without altering a single error chain.
var errInvalidPadding = errors.New("poseidon/bufx: pad length exceeds payload")

// StripPadding parses a padded frame payload (DATA, HEADERS, PUSH_PROMISE).
// raw[0] is the pad length; raw[1:1+actualLen] is the real payload;
// raw[1+actualLen:] is padding. Returned payload aliases raw — caller must
// respect the visitor lifetime contract.
func StripPadding(raw []byte) (payload []byte, padLen uint8, err error) {
	if len(raw) < 1 {
		return nil, 0, errInvalidPadding
	}
	padLen = raw[0]
	if int(padLen) > len(raw)-1 {
		return nil, 0, errInvalidPadding
	}
	return raw[1 : len(raw)-int(padLen)], padLen, nil
}
