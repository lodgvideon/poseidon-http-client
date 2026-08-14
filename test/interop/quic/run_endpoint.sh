#!/bin/bash
# Endpoint entrypoint for quic-interop-runner. The runner starts this container
# with ROLE, TESTCASE and REQUESTS in the environment; /logs is copied out
# afterwards and /downloads is compared against the server's source directory.
set -e

# Routing for the ns-3 simulator, tx checksum offload off (ns-3 needs valid
# checksums), and the /logs and /logs/qlog directories. Provided by the base
# image; it has to run before anything touches the network.
/setup.sh

if [ "$ROLE" == "client" ]; then
    # The simulator must be listening before the first Initial goes out,
    # otherwise it is dropped before the test has begun.
    /wait-for-it.sh sim:57832 -s -t 30

    echo "Test case:     $TESTCASE"
    echo "Client params: $CLIENT_PARAMS"
    echo "Requests:      $REQUESTS"

    # Both are whitespace-separated argument lists rather than single arguments,
    # so neither is quoted. The TESTCASE dispatch — and the exit 127 that tells
    # the runner a case is unsupported — lives inside the binary, which is what
    # makes it reachable during the compliance check, where there is no server,
    # no request list and a deliberately unknown TESTCASE.
    # shellcheck disable=SC2086
    ./interop-client $CLIENT_PARAMS $REQUESTS
else
    # This image registers with role "client", so the runner never asks it to
    # serve. 127 is the runner's "not implemented" code, which is the honest
    # answer if it ever does.
    echo "poseidon is a client-only endpoint; ROLE=$ROLE is not implemented" >&2
    exit 127
fi
