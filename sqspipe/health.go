package main

import (
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type healthStat struct {
	id   int
	last time.Time
	lock sync.RWMutex
}

func (h *healthStat) update() {
	now := time.Now()
	h.lock.Lock()
	h.last = now
	h.lock.Unlock()
}

func (h *healthStat) get() bool {
	h.lock.RLock()
	last := h.last
	h.lock.RUnlock()
	elap := time.Since(last)

	// sqs listener will update status at every 20 seconds at most
	healthy := elap < 25*time.Second

	log.Printf("healthStat[%d] get: elap=%v healthy=%v", h.id, elap, healthy)
	return healthy
}

func startHealthEndpoint(app appConfig) chan struct{} {

	log.Printf("startHealthEndpoint: addr=%s path=%s", app.healthAddr, app.healthPath)

	http.HandleFunc(app.healthPath, func(w http.ResponseWriter, r *http.Request) {
		healthy := true
		for i := 0; i < app.readers; i++ {
			if !app.health[i].get() {
				healthy = false
				break
			}
		}

		if healthy {
			io.WriteString(w, "200 server ok\n")
			return
		}

		http.Error(w, "500 server failing", 500)
	})

	done := make(chan struct{})
	go listenAndServe(done, app.healthAddr, nil)
	return done
}

func listenAndServe(done chan struct{}, addr string, handler http.Handler) {
	server := &http.Server{Addr: addr, Handler: handler}
	log.Printf("listenAndServe: addr=%s", addr)
	err := server.ListenAndServe()
	log.Printf("listenAndServe: addr=%s error: %v", addr, err)
	done <- struct{}{}
}
