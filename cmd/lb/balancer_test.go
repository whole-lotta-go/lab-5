package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestChooseHealthy(t *testing.T) {
	healthMutex.Lock()
	healthStatus = map[string]bool{
		"server1:8080": true,
		"server2:8080": false,
		"server3:8080": true,
	}
	serversPool = []string{"server1:8080", "server2:8080", "server3:8080"}
	healthMutex.Unlock()

	req := &http.Request{
		RemoteAddr: "192.168.1.10:12345",
	}

	server, err := chooseHealthy(req)
	if err != nil {
		t.Fatalf("Expected a healthy server, got error: %v", err)
	}

	if server != "server1:8080" && server != "server3:8080" {
		t.Errorf("Unexpected server selected: %s", server)
	}
}

func TestChooseHealthyNoHealthyServers(t *testing.T) {
	healthMutex.Lock()
	healthStatus = map[string]bool{
		"server1:8080": false,
		"server2:8080": false,
		"server3:8080": false,
	}
	healthMutex.Unlock()

	req := &http.Request{
		RemoteAddr: "192.168.1.10:12345",
	}

	_, err := chooseHealthy(req)
	if err == nil {
		t.Fatal("Expected error due to no healthy servers, got none")
	}
}

func TestForwardSuccess(t *testing.T) {
	// Mock backend server
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Test", "ok")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("response from backend"))
	}))
	defer backend.Close()

	// Extract host part (since our forward() expects host only)
	host := strings.TrimPrefix(backend.URL, "http://")

	// Create dummy request to be forwarded
	req := httptest.NewRequest(http.MethodGet, "http://localhost/", nil)

	// Create ResponseRecorder to capture output
	rr := httptest.NewRecorder()

	err := forward(host, rr, req)
	if err != nil {
		t.Fatalf("Expected no error from forward, got: %v", err)
	}

	resp := rr.Result()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	if string(body) != "response from backend" {
		t.Errorf("Unexpected response body: %s", string(body))
	}

	if got := resp.Header.Get("X-Test"); got != "ok" {
		t.Errorf("Expected X-Test header to be 'ok', got '%s'", got)
	}
}

func TestForwardError(t *testing.T) {
	// Simulate non-responsive backend
	badServer := "localhost:65534" // Unused port

	req := httptest.NewRequest(http.MethodGet, "http://localhost/", nil)
	rr := httptest.NewRecorder()

	err := forward(badServer, rr, req)
	if err == nil {
		t.Fatal("Expected error from forward to bad server, got nil")
	}

	resp := rr.Result()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("Expected status 503, got %d", resp.StatusCode)
	}
}

