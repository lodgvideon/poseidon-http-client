package grpc

import (
	"fmt"
	"strconv"
	"time"
)

// decodeTimeout parses a grpc-timeout field value. It lives in the test tree
// because a gRPC client only ever writes this header — nothing in the
// production path reads one back. It exists so tests can assert that what
// encodeTimeout produced is a value a server could actually parse.
func decodeTimeout(v string) (time.Duration, error) {
	if len(v) < 2 {
		return 0, fmt.Errorf("grpc: malformed grpc-timeout %q", v)
	}
	n, err := strconv.ParseInt(v[:len(v)-1], 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("grpc: malformed grpc-timeout %q", v)
	}
	for _, u := range timeoutUnits {
		if u.suffix == v[len(v)-1] {
			return time.Duration(n) * u.size, nil
		}
	}
	return 0, fmt.Errorf("grpc: unknown grpc-timeout unit in %q", v)
}
