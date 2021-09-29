package main

import (
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

type readerHealthStat struct {
	id   int
	last time.Time
	lock sync.RWMutex
}

func (h *readerHealthStat) update() {
	now := time.Now()
	h.lock.Lock()
	h.last = now
	h.lock.Unlock()
}

func (h *readerHealthStat) get() bool {
	h.lock.RLock()
	last := h.last
	h.lock.RUnlock()
	elap := time.Since(last)

	// sqs listener will update status at every 20 seconds at most
	healthy := elap < 25*time.Second

	log.Printf("readerHealthStat[%d] get: elap=%v healthy=%v", h.id, elap, healthy)
	return healthy
}

type writerHealthStat struct {
	id   int
	last bool
	lock sync.RWMutex
}

func (h *writerHealthStat) update(good bool) {
	h.lock.Lock()
	h.last = good
	h.lock.Unlock()
}

func (h *writerHealthStat) get() bool {
	h.lock.RLock()
	last := h.last
	h.lock.RUnlock()

	log.Printf("writerHealthStat[%d] get: healthy=%v", h.id, last)
	return last
}

func startHealthEndpoint(app appConfig) chan struct{} {

	log.Printf("startHealthEndpoint: addr=%s path=%s", app.healthAddr, app.healthPath)

	http.HandleFunc(app.healthPath, func(w http.ResponseWriter, r *http.Request) {
		healthy := scanHealth(app)

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

func scanHealth(app appConfig) bool {
	for i := 0; i < app.readers; i++ {
		if !app.readerHealth[i].get() {
			return false
		}
	}

	for i := 0; i < app.writers; i++ {
		if !app.writerHealth[i].get() {
			return false
		}
	}

	return true
}

func listenAndServe(done chan struct{}, addr string, handler http.Handler) {
	server := &http.Server{Addr: addr, Handler: handler}
	log.Printf("listenAndServe: addr=%s", addr)
	err := server.ListenAndServe()
	log.Printf("listenAndServe: addr=%s error: %v", addr, err)
	done <- struct{}{}
}
