package main

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/lodgvideon/poseidon-http-client/http3"
)

// h3DownloadAll serves the "http3" test case: every URL fetched over one HTTP/3
// connection with the requests in flight together, which is what the case exists
// to exercise (the runner asks for three files of 5 KB, 10 KB and 500 KB in
// parallel on a single connection).
//
// http3.Dial sets the "h3" ALPN and the TLS 1.3 floor itself, so the config
// passed in carries only the key log and the ServerName.
func h3DownloadAll(ctx context.Context, j *job) error {
	urls, err := parseURLs(j.urls)
	if err != nil {
		return err
	}
	cfg := j.tlsConfig.Clone()
	cfg.ServerName = urls[0].Hostname()

	client, err := http3.Dial(ctx, hostPort(urls[0]), cfg)
	if err != nil {
		return fmt.Errorf("dial %s: %w", hostPort(urls[0]), err)
	}
	defer func() { _ = client.Close() }()

	return inParallel(urls, func(u *url.URL) error { return h3Get(ctx, client, u, j.downloads) })
}

// h3Get performs one HTTP/3 GET and writes the response body to dir. The body is
// buffered rather than streamed because this case's largest file is 500 KB; the
// bulk transfers all arrive as "transfer", which is HTTP/0.9 and streams.
func h3Get(ctx context.Context, client *http3.Client, u *url.URL, dir string) error {
	resp, body, err := client.Do(ctx, &http3.Request{
		Method:    "GET",
		Scheme:    "https",
		Authority: u.Host,
		Path:      u.RequestURI(),
	})
	if err != nil {
		return fmt.Errorf("GET %s: %w", u.Path, err)
	}
	if resp.Status != 200 {
		return fmt.Errorf("GET %s: status %d", u.Path, resp.Status)
	}
	// 0o644, not the 0o600 gosec's G306 wants: the runner's _check_files step
	// reads these back from the host, where it runs as the invoking user while
	// this container runs as root, and an owner-only file fails the comparison
	// with EACCES before a single byte is diffed. The HTTP/0.9 path reaches the
	// same mode through os.Create (0o666 & ^umask) and never had the problem, so
	// this is the two download paths agreeing rather than a relaxation. Nothing
	// secret is written here — the bodies are the runner's own random fixture
	// files, fetched to be compared byte-for-byte against the server's copies.
	// The SSLKEYLOGFILE in openKeyLog does hold TLS secrets and stays 0o600.
	//nolint:gosec // G306: see above — the harness must read these back as a
	// non-root user, and the content is a throwaway fixture, not a secret.
	if err := os.WriteFile(downloadPath(dir, u), body, 0o644); err != nil {
		return fmt.Errorf("write download for %s: %w", u.Path, err)
	}
	return nil
}
