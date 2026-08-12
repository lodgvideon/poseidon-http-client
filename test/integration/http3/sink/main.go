// Command sink is an upload target that actually consumes the request body.
//
// It exists because the congestion-control benchmark was measuring nothing
// (#564): it posted its payload to Caddy's "/" route, which is a canned
// `respond` that answers on the request headers and never reads the body. The
// client's Do returned on that response, so the timer closed while the payload
// was still buffered — elapsed time came out independent of payload size, and
// the derived goodput reached a physically impossible 11 GiB/s.
//
// Caddy cannot be configured out of that on its own: neither `respond` nor
// `file_server` drains a request body. `reverse_proxy` does, because it has to
// forward the bytes — so Caddy fronts this sink, keeps speaking HTTP/3 to the
// client, and the upload becomes real.
//
// The byte count is logged per request so a run can prove the payload arrived
// rather than assume it. A benchmark that cannot show what it received is how
// this bug survived in the first place.
package main

import (
	"io"
	"log"
	"net/http"
	"os"
)

func main() {
	addr := os.Getenv("SINK_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		n, err := io.Copy(io.Discard, r.Body)
		if err != nil {
			log.Printf("SINK received=%d err=%v", n, err)
			http.Error(w, "short read", http.StatusBadRequest)
			return
		}
		log.Printf("SINK received=%d", n)
		w.WriteHeader(http.StatusNoContent)
	})
	srv := &http.Server{Addr: addr, Handler: mux}
	log.Printf("sink listening on %s", addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
