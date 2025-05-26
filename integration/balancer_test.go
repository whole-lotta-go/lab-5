package integration

import (
	"fmt"
	"net/http"
	"os"
	"testing"
	"time"
)

const (
	baseAddress = "http://balancer:8090"
	apiPath     = "/api/v1/some-data"
)

var client = http.Client{
	Timeout: 3 * time.Second,
}

func buildUrl(path string) string {
	return fmt.Sprintf("%s%s", baseAddress, path)
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	t.Run("BasicConnectivity", testBasicConnectivity)
	t.Run("LoadDistribution", testLoadDistribution)
	t.Run("ConsistentHashing", testConsistentHashing)
	t.Run("ErrorHandling", testErrorHandling)
}

func testBasicConnectivity(t *testing.T) {
	resp, err := client.Get(buildUrl(apiPath))
	if err != nil {
		t.Fatalf("Failed to get response: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	lbFrom := resp.Header.Get("lb-from")
	if lbFrom == "" {
		t.Error("Expected lb-from header")
	}
	t.Logf("response from [%s]", lbFrom)
}

func testLoadDistribution(t *testing.T) {
	serverHits := make(map[string]int)

	for i := 0; i < 20; i++ {
		resp, err := client.Get(buildUrl(apiPath))
		if err != nil {
			t.Errorf("Request %d failed: %v", i, err)
			continue
		}
		server := resp.Header.Get("lb-from")
		if server != "" {
			serverHits[server]++
		}
		resp.Body.Close()
	}

	if len(serverHits) == 0 {
		t.Fatal("No servers responded")
	}

	t.Log("Load distribution:")
	for server, hits := range serverHits {
		t.Logf("  %s: %d hits", server, hits)
	}
}

func testConsistentHashing(t *testing.T) {
	firstServer := ""

	for i := 0; i < 5; i++ {
		resp, err := client.Get(buildUrl(apiPath))
		if err != nil {
			t.Errorf("Consistency test request %d failed: %v", i, err)
			continue
		}

		server := resp.Header.Get("lb-from")
		if i == 0 {
			firstServer = server
		} else if server != firstServer {
			t.Errorf("Inconsistent routing: expected %s, got %s", firstServer, server)
		}
		resp.Body.Close()
	}

	t.Logf("Consistent routing to: %s", firstServer)
}

func testErrorHandling(t *testing.T) {
	resp, err := client.Get(buildUrl("/nonexistent-endpoint"))
	if err != nil {
		t.Logf("Expected error for nonexistent endpoint: %v", err)
	} else {
		t.Logf("Nonexistent endpoint returned status: %d", resp.StatusCode)
		resp.Body.Close()
	}
}

func BenchmarkBalancer(b *testing.B) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		b.Skip("Integration test is not enabled")
	}

	b.Run("Sequential", benchmarkSequential)
	b.Run("Parallel", benchmarkParallel)
	b.Run("MultipleEndpoints", benchmarkMultipleEndpoints)
	b.Run("Throughput", benchmarkThroughput)
}

func benchmarkSequential(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(buildUrl(apiPath))
		if err != nil {
			b.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()
	}
}

func benchmarkParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(buildUrl(apiPath))
			if err != nil {
				b.Fatalf("Request failed: %v", err)
			}
			resp.Body.Close()
		}
	})
}

func benchmarkMultipleEndpoints(b *testing.B) {
	endpoints := []string{
		apiPath,
		"/api/v1/users",
		"/api/v1/health",
	}

	for i := 0; i < b.N; i++ {
		endpoint := endpoints[i%len(endpoints)]
		resp, err := client.Get(buildUrl(endpoint))
		if err != nil {
			b.Fatalf("Request to %s failed: %v", endpoint, err)
		}
		resp.Body.Close()
	}
}

func benchmarkThroughput(b *testing.B) {
	start := time.Now()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := client.Get(buildUrl(apiPath))
		if err != nil {
			b.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()
	}

	duration := time.Since(start)
	b.ReportMetric(float64(b.N)/duration.Seconds(), "requests/sec")
}
