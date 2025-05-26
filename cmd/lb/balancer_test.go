package main

import (
	"hash/fnv"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBalancer(t *testing.T) {
	t.Run("healthy servers selection", func(t *testing.T) {
		serversPool = []string{
			"server1:8080",
			"server2:8080",
			"server3:8080",
		}

		healthMutex.Lock()
		healthStatus = map[string]bool{
			"server1:8080": true,
			"server2:8080": false,
			"server3:8080": true,
		}
		healthMutex.Unlock()

		req := httptest.NewRequest("GET", "/", nil)
		req.RemoteAddr = "192.168.1.1:12345"

		rr := httptest.NewRecorder()

		handler := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			healthMutex.RLock()
			var healthyServers []string
			for _, s := range serversPool {
				if healthy, ok := healthStatus[s]; ok && healthy {
					healthyServers = append(healthyServers, s)
				}
			}
			healthMutex.RUnlock()

			if len(healthyServers) == 0 {
				http.Error(rw, "No healthy servers available", http.StatusServiceUnavailable)
				return
			}

			clientIP, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				clientIP = r.RemoteAddr
			}

			h := fnv.New32a()
			h.Write([]byte(clientIP))
			hashValue := h.Sum32()
			index := int(hashValue) % len(healthyServers)
			selectedServer := healthyServers[index]

			rw.Header().Set("lb-server", selectedServer)
		})

		handler.ServeHTTP(rr, req)

		selectedServer := rr.Header().Get("lb-server")
		if selectedServer != "server1:8080" && selectedServer != "server3:8080" {
			t.Errorf("Expected server1:8080 or server3:8080, got %s", selectedServer)
		}
	})

	t.Run("no healthy servers available", func(t *testing.T) {

		healthMutex.Lock()
		healthStatus = map[string]bool{
			"server1:8080": false,
			"server2:8080": false,
			"server3:8080": false,
		}
		healthMutex.Unlock()

		req := httptest.NewRequest("GET", "/", nil)
		rr := httptest.NewRecorder()

		handler := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			healthMutex.RLock()
			var healthyServers []string
			for _, s := range serversPool {
				if healthy, ok := healthStatus[s]; ok && healthy {
					healthyServers = append(healthyServers, s)
				}
			}
			healthMutex.RUnlock()

			if len(healthyServers) == 0 {
				http.Error(rw, "No healthy servers available", http.StatusServiceUnavailable)
				return
			}
		})

		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusServiceUnavailable {
			t.Errorf("Expected status %d, got %d", http.StatusServiceUnavailable, rr.Code)
		}
	})

	t.Run("consistent server selection for same IP", func(t *testing.T) {

		healthMutex.Lock()
		healthStatus = map[string]bool{
			"server1:8080": true,
			"server2:8080": true,
			"server3:8080": true,
		}
		healthMutex.Unlock()

		testIP := "192.168.1.100:54321"

		req1 := httptest.NewRequest("GET", "/", nil)
		req1.RemoteAddr = testIP
		rr1 := httptest.NewRecorder()

		handler := http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
			healthMutex.RLock()
			var healthyServers []string
			for _, s := range serversPool {
				if healthy, ok := healthStatus[s]; ok && healthy {
					healthyServers = append(healthyServers, s)
				}
			}
			healthMutex.RUnlock()

			clientIP, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				clientIP = r.RemoteAddr
			}

			h := fnv.New32a()
			h.Write([]byte(clientIP))
			hashValue := h.Sum32()
			index := int(hashValue) % len(healthyServers)
			selectedServer := healthyServers[index]

			rw.Header().Set("lb-server", selectedServer)
		})

		handler.ServeHTTP(rr1, req1)
		firstServer := rr1.Header().Get("lb-server")

		req2 := httptest.NewRequest("GET", "/", nil)
		req2.RemoteAddr = testIP
		rr2 := httptest.NewRecorder()
		handler.ServeHTTP(rr2, req2)
		secondServer := rr2.Header().Get("lb-server")

		if firstServer != secondServer {
			t.Errorf("Expected same server for same IP, got %s and %s", firstServer, secondServer)
		}
	})
}
