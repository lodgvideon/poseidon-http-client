package bytesx

import "errors"

// ErrInvalidPadding is returned when the declared pad length exceeds the
// remaining payload (RFC 7540 §6.1: PROTOCOL_ERROR).
var ErrInvalidPadding = errors.New("poseidon/bytesx: pad length exceeds payload")

// StripPadding parses a padded frame payload (DATA, HEADERS, PUSH_PROMISE).
// raw[0] is the pad length; raw[1:1+actualLen] is the real payload;
// raw[1+actualLen:] is padding. Returned payload aliases raw — caller must
// respect the visitor lifetime contract.
func StripPadding(raw []byte) (payload []byte, padLen uint8, err error) {
	if len(raw) < 1 {
		return nil, 0, ErrInvalidPadding
	}
	padLen = raw[0]
	if int(padLen) > len(raw)-1 {
		return nil, 0, ErrInvalidPadding
	}
	return raw[1 : len(raw)-int(padLen)], padLen, nil
}
