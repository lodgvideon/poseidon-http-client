package io.poseidon.it;

import io.undertow.Undertow;
import io.undertow.server.handlers.PathHandler;
import io.undertow.server.handlers.BlockingHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.UndertowOptions;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Deque;
import java.util.zip.GZIPOutputStream;

public final class Main {

    public static void main(String[] args) throws Exception {
        String certPath = System.getenv().getOrDefault("CERT_PATH", "/app/certs/server.pem");
        String keyPath  = System.getenv().getOrDefault("KEY_PATH",  "/app/certs/server.key");

        SSLContext sslContext = buildSSLContext(certPath, keyPath);

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
            .addHttpsListener(8443, "0.0.0.0", sslContext)
            .addHttpListener(8080, "0.0.0.0")
            .setHandler(routes)
            .build();

        System.out.println("[undertow] Starting on :8443 (h2) + :8080 (h2c)");
        server.start();
        System.out.println("[undertow] Ready");
        Thread.currentThread().join();
    }

    // ── SSL helpers ───────────────────────────────────────────────

    private static SSLContext buildSSLContext(String certPath, String keyPath) throws Exception {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        X509Certificate cert = (X509Certificate) cf.generateCertificate(
            Files.newInputStream(Path.of(certPath)));

        PrivateKey key = loadPrivateKey(keyPath);

        KeyStore ks = KeyStore.getInstance("PKCS12");
        ks.load(null, new char[0]);
        ks.setKeyEntry("server", key, new char[0], new Certificate[]{cert});

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(
            KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(ks, new char[0]);

        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(kmf.getKeyManagers(), null, null);
        return ctx;
    }

    private static PrivateKey loadPrivateKey(String path) throws Exception {
        String pem = Files.readString(Path.of(path));
        // Strip PEM header/footer lines (-----BEGIN/END ... KEY-----)
        String b64 = pem.replaceAll("-----[A-Z ]+-----", "").replaceAll("\\s", "");
        byte[] der = Base64.getDecoder().decode(b64);
        try {
            PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(der);
            return KeyFactory.getInstance("RSA").generatePrivate(spec);
        } catch (Exception e) {
            // Try PKCS1 → wrap in PKCS8
            byte[] pkcs8 = wrapPKCS1(der);
            return KeyFactory.getInstance("RSA").generatePrivate(new PKCS8EncodedKeySpec(pkcs8));
        }
    }

    private static byte[] wrapPKCS1(byte[] pkcs1Der) {
        // Minimal PKCS1→PKCS8 wrapper for RSA keys
        byte[] header = new byte[]{
            0x30, (byte)(0x82), 0, 0, // SEQUENCE, length placeholder
            0x02, 0x01, 0x00,         // INTEGER 0 (version)
            0x30, 0x0D,               // SEQUENCE (algorithm)
            0x06, 0x09, 0x2A, (byte)0x86, 0x48, (byte)0x86, (byte)0xF7, 0x0D, 0x01, 0x01, 0x01, // OID rsaEncryption
            0x05, 0x00,               // NULL
            0x04, (byte)(0x82), 0, 0  // OCTET STRING, length placeholder
        };
        int totalLen = header.length + pkcs1Der.length;
        header[2] = (byte)((totalLen - 4) >> 8);
        header[3] = (byte)((totalLen - 4) & 0xFF);
        header[header.length - 2] = (byte)(pkcs1Der.length >> 8);
        header[header.length - 1] = (byte)(pkcs1Der.length & 0xFF);

        byte[] out = new byte[header.length + pkcs1Der.length];
        System.arraycopy(header, 0, out, 0, header.length);
        System.arraycopy(pkcs1Der, 0, out, header.length, pkcs1Der.length);
        return out;
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
        ex.getResponseHeaders().put(new HttpString("X-Trailer-Foo"), "bar");
    }

    private static void never(HttpServerExchange ex) {
        try { Thread.sleep(300_000); } catch (InterruptedException ignored) {}
    }
}
