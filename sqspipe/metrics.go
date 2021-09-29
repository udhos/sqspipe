package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type metrics struct {
	readMessage prometheus.Counter
	readOk      prometheus.Counter
	readEmpty   prometheus.Counter
	readError   prometheus.Counter
	writeOk     prometheus.Counter
	writeError  prometheus.Counter
	deleteOk    prometheus.Counter
	deleteError prometheus.Counter
}

func newMetrics() *metrics {

	namespace := basename

	m := metrics{
		readMessage: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "read_message_total",
				Help:      "Number of messages read successfully.",
			},
		),

		readOk: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "read_ok_total",
				Help:      "Number of successful SQS listener ReceiveMessage calls.",
			},
		),

		readEmpty: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "read_empty_total",
				Help:      "Number of empty SQS listener ReceiveMessage calls.",
			},
		),

		readError: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "read_error_total",
				Help:      "Number of SQS listener ReceiveMessage errors.",
			},
		),

		writeOk: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "write_ok_total",
				Help:      "Number of successful SQS SendMessage calls.",
			},
		),

		writeError: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "write_error_total",
				Help:      "Number of SQS SendMessage errors.",
			},
		),

		deleteOk: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "delete_ok_total",
				Help:      "Number of successful SQS DeleteMessage calls.",
			},
		),

		deleteError: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: namespace,
				Name:      "delete_error_total",
				Help:      "Number of SQS DeleteMessage errors.",
			},
		),
	}

	prometheus.MustRegister(m.readMessage)
	prometheus.MustRegister(m.readOk)
	prometheus.MustRegister(m.readEmpty)
	prometheus.MustRegister(m.readError)
	prometheus.MustRegister(m.writeOk)
	prometheus.MustRegister(m.writeError)
	prometheus.MustRegister(m.deleteOk)
	prometheus.MustRegister(m.deleteError)

	return &m
}

func serveMetrics(addr, path string) {
	log.Printf("serveMetrics: addr=%s path=%s", addr, path)
	http.Handle(path, promhttp.Handler())
	log.Fatal(http.ListenAndServe(addr, nil))
}
