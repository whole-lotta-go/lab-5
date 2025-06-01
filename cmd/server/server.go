package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/roman-mazur/architecture-practice-4-template/httptools"
	"github.com/roman-mazur/architecture-practice-4-template/signal"
)

var port = flag.Int("port", 8080, "server port")

const (
	confResponseDelaySec = "CONF_RESPONSE_DELAY_SEC"
	confHealthFailure    = "CONF_HEALTH_FAILURE"
	dbBaseURL            = "http://db:8070"
	teamName             = "wholelottago"
)

func main() {
	h := new(http.ServeMux)

	h.HandleFunc("/health", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("content-type", "text/plain")
		if failConfig := os.Getenv(confHealthFailure); failConfig == "true" {
			rw.WriteHeader(http.StatusInternalServerError)
			_, _ = rw.Write([]byte("FAILURE"))
		} else {
			rw.WriteHeader(http.StatusOK)
			_, _ = rw.Write([]byte("OK"))
		}
	})

	report := make(Report)

	h.HandleFunc("/api/v1/some-data", func(rw http.ResponseWriter, r *http.Request) {
		respDelayString := os.Getenv(confResponseDelaySec)
		if delaySec, parseErr := strconv.Atoi(respDelayString); parseErr == nil && delaySec > 0 && delaySec < 300 {
			time.Sleep(time.Duration(delaySec) * time.Second)
		}

		report.Process(r)

		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(rw, "Missing 'key' parameter", http.StatusBadRequest)
			return
		}

		value, err := getFromDB(key)
		if err != nil {
			if strings.Contains(err.Error(), "status 404") {
				rw.WriteHeader(http.StatusNotFound)
			} else {
				http.Error(rw, "Internal server error", http.StatusInternalServerError)
			}
			return
		}

		rw.Header().Set("content-type", "application/json")
		rw.WriteHeader(http.StatusOK)
		json.NewEncoder(rw).Encode(value)
	})

	h.Handle("/report", report)

	server := httptools.CreateServer(*port, h)
	server.Start()

	if err := waitForDB(); err != nil {
		log.Printf("Failed to wait for DB: %v", err)
	} else {
		saveCurrentDate()
	}

	signal.WaitForTerminationSignal()
}

func waitForDB() error {
	timeout := time.After(10 * time.Second)
	tick := time.Tick(500 * time.Millisecond)

	for {
		select {
		case <-timeout:
			return fmt.Errorf("DB service timeout")
		case <-tick:
			resp, err := http.Get(dbBaseURL + "/health")
			if err == nil && resp.StatusCode == http.StatusOK {
				return nil
			}
		}
	}
}

func saveCurrentDate() {
	currentDate := time.Now().Format("2006-01-02")
	url := dbBaseURL + "/db/" + teamName

	body, err := json.Marshal(map[string]string{"value": currentDate})
	if err != nil {
		log.Printf("JSON marshal error: %v", err)
		return
	}

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
	if err != nil {
		log.Printf("Failed to send request to DB: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf("DB returned status %d", resp.StatusCode)
	}
}

func getFromDB(key string) (string, error) {
	resp, err := http.Get(dbBaseURL + "/db/" + key)
	if err != nil {
		return "", fmt.Errorf("failed to send request to DB: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", fmt.Errorf("status 404")
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("DB returned status %d", resp.StatusCode)
	}

	var result struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to decode response: %w", err)
	}

	return result.Value, nil
}
