package main

import (
	"log"
	"os"
	"strconv"
)

func stringFromEnv(name string, defaultValue string) string {
	str := os.Getenv(name)
	if str != "" {
		log.Printf("%s=[%s] using %s=%s default=%s", name, str, name, str, defaultValue)
		return str
	}
	log.Printf("%s=[%s] using %s=%s default=%s", name, str, name, defaultValue, defaultValue)
	return defaultValue
}

func valueFromEnv(name string, defaultValue int) int {
	str := os.Getenv(name)
	if str != "" {
		value, errConv := strconv.Atoi(str)
		if errConv == nil {
			log.Printf("%s=[%s] using %s=%d default=%d", name, str, name, value, defaultValue)
			return value
		}
		log.Fatalf("bad %s=[%s]: error: %v", name, str, errConv)
		os.Exit(1)
	}
	log.Printf("%s=[%s] using %s=%d default=%d", name, str, name, defaultValue, defaultValue)
	return defaultValue
}

func getEnv(name string) string {
	value := os.Getenv(name)
	log.Printf("%s=[%s]", name, value)
	return value
}

func requireEnv(name string) string {
	value := getEnv(name)
	if value == "" {
		log.Fatalf("requireEnv: error: please set env var: %s", name)
		os.Exit(1)
		return ""
	}

	return value
}
