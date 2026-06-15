//go:build integration

// Package integration_test provides cross-implementation HTTP/2 integration
// tests for poseidon-http-client against Go net/http, nginx, Undertow, and
// nghttpx — all running locally (in-process or via docker-compose).
package integration_test
