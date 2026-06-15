package io.poseidon.it;

import io.undertow.Undertow;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.BlockingHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.UndertowOptions;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import io.undertow.util.StatusCodes;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Deque;
import java.util.zip.GZIPOutputStream;

/**
 * Minimal Undertow HTTP/2 server for poseidon-http-client integration tests.
 *
 * Endpoints (must match test/integration/fixtures/CONTRACT.md):
 *   GET  /healthz         → 200 "ok"
 *   ANY  /echo            → echoes request body + headers
 *   GET  /large?bytes=N   → N-byte zero-filled body
 *   GET  /status/{code}   → returns HTTP status {code}
 *   GET  /delay?ms=N      → sleeps N ms, then 200
 *   GET  /chunked         → streams 100 × 1KB chunks with 10ms delay
 *   GET  /gzip            → 100KB gzip-compressed body
 *   GET  /trailers        → response with trailer headers
 *   GET  /never           → hangs forever (for cancel tests)
 */
public final class Main {

    public static void main(String[] args) throws Exception {

        String certPath = System.getenv().getOrDefault("CERT_PATH", "/app/certs/server.pem");
        String keyPath  = System.getenv().getOrDefault("KEY_PATH",  "/app/certs/server.key");

        PathHandler routes = new PathHandler()
            .addExactPath("/healthz", new BlockingHandler(Main::healthz))
            .addExactPath("/echo",    new BlockingHandler(Main::echo))
            .addPrefixPath("/large",  new BlockingHandler(Main::large))
            .addPrefixPath("/status", new BlockingHandler(Main::status))
            .addPrefixPath("/delay",  new BlockingHandler(Main::delay))
            .addExactPath("/chunked", new BlockingHandler(Main::chunked))
            .addExactPath("/gzip",    new BlockingHandler(Main::gzip))
            .addExactPath("/trailers",new BlockingHandler(Main::trailers))
            .addExactPath("/never",   new BlockingHandler(Main::never))
            .addExactPath("/",        new BlockingHandler(Main::root));

        Undertow server = Undertow.builder()
            .setServerOption(UndertowOptions.ENABLE_HTTP2, true)
            .setServerOption(UndertowOptions.HTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, 128)
            .addHttpsListener(8443, "0.0.0.0",
                Main.class.getResourceAsStream("/server.pem"),
                Main.class.getResourceAsStream("/server.key"))
            .addHttpListener(8080, "0.0.0.0")   // h2c prior-knowledge
            .setHandler(routes)
            .build();

        System.out.println("[undertow] Starting on :8443 (h2) + :8080 (h2c)");
        server.start();
        System.out.println("[undertow] Ready");

        // Block forever
        Thread.currentThread().join();
    }

    // ── Handlers ──────────────────────────────────────────────────

    private static void root(HttpServerExchange ex) {
        ex.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
        ex.getResponseSender().send("hello from undertow");
    }

    private static void healthz(HttpServerExchange ex) {
        ex.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
        ex.getResponseSender().send("ok");
    }

    private static void echo(HttpServerExchange ex) {
        ex.startBlocking();
        var headers = ex.getRequestHeaders();
        StringBuilder sb = new StringBuilder();
        for (var h : headers) {
            sb.append(h.getHeaderName()).append(": ").append(h.getFirst()).append("\n");
        }
        // Echo body
        byte[] body;
        try {
            body = ex.getInputStream().readAllBytes();
        } catch (Exception e) {
            body = new byte[0];
        }
        ex.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
        ex.getResponseHeaders().put(new HttpString("X-Echo-Headers"), sb.toString().trim());
        ex.getResponseSender().send(new String(body, StandardCharsets.UTF_8));
    }

    private static void large(HttpServerExchange ex) {
        Deque<String> p = ex.getQueryParameters().get("bytes");
        int n = (p != null) ? Integer.parseInt(p.getFirst()) : 1048576;
        ex.getResponseHeaders().put(Headers.CONTENT_TYPE, "application/octet-stream");
        ex.getResponseHeaders().put(Headers.CONTENT_LENGTH, String.valueOf(n));
        ex.startBlocking();
        try {
            OutputStream os = ex.getOutputStream();
            byte[] chunk = new byte[4096];
            int sent = 0;
            while (sent < n) {
                int sz = Math.min(4096, n - sent);
                os.write(chunk, 0, sz);
                sent += sz;
            }
            os.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void status(HttpServerExchange ex) {
        String path = ex.getRelativePath().substring("/status/".length());
        int code = Integer.parseInt(path);
        ex.setStatusCode(code);
        ex.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
        ex.getResponseSender().send("status " + code);
    }

    private static void delay(HttpServerExchange ex) {
        Deque<String> p = ex.getQueryParameters().get("ms");
        int ms = (p != null) ? Integer.parseInt(p.getFirst()) : 1000;
        try { Thread.sleep(ms); } catch (InterruptedException ignored) {}
        ex.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
        ex.getResponseSender().send("delayed " + ms + "ms");
    }

    private static void chunked(HttpServerExchange ex) {
        ex.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
        ex.startBlocking();
        try {
            OutputStream os = ex.getOutputStream();
            byte[] chunk = new byte[1024];
            for (int i = 0; i < 100; i++) {
                os.write(chunk);
                os.flush();
                Thread.sleep(10);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void gzip(HttpServerExchange ex) {
        ex.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
        ex.getResponseHeaders().put(Headers.CONTENT_ENCODING, "gzip");
        ex.startBlocking();
        try {
            GZIPOutputStream gz = new GZIPOutputStream(ex.getOutputStream());
            byte[] chunk = "x".repeat(1024).getBytes(StandardCharsets.UTF_8);
            for (int i = 0; i < 100; i++) {
                gz.write(chunk);
            }
            gz.finish();
            gz.flush();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void trailers(HttpServerExchange ex) {
        ex.getResponseHeaders().put(Headers.CONTENT_TYPE, "text/plain");
        ex.getResponseHeaders().put(new HttpString("Trailer"), "X-Trailer-Foo");
        ex.getResponseSender().send("trailers");
        // Undertow supports trailers via exchange.endExchange() + trailer headers
        ex.getResponseHeaders().put(new HttpString("X-Trailer-Foo"), "bar");
    }

    private static void never(HttpServerExchange ex) {
        try { Thread.sleep(300_000); } catch (InterruptedException ignored) {}
    }
}
