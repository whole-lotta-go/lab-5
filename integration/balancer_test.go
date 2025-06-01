package integration

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

const (
	baseAddress = "http://balancer:8090"
	apiPath     = "/api/v1/some-data"
)

var (
	client = http.Client{
		Timeout: 3 * time.Second,
	}

	currentDate = time.Now().Format("2006-01-02")
)

func buildUrl(path string) string {
	return fmt.Sprintf("%s%s", baseAddress, path)
}

func apiUrlWithKey() string {
	return buildUrl(apiPath) + "?key=wholelottago"
}

func TestBalancer(t *testing.T) {
	if _, exists := os.LookupEnv("INTEGRATION_TEST"); !exists {
		t.Skip("Integration test is not enabled")
	}

	t.Run("BasicConnectivity", testBasicConnectivity)
	t.Run("DatabaseIntegration", testDatabaseIntegration)
	t.Run("LoadDistribution", testLoadDistribution)
	t.Run("ConsistentHashing", testConsistentHashing)
	t.Run("ErrorHandling", testErrorHandling)
	t.Run("InvalidKey", testInvalidKey)
	t.Run("MissingKey", testMissingKey)
}

func testBasicConnectivity(t *testing.T) {
	resp, err := client.Get(apiUrlWithKey())
	if err != nil {
		t.Fatalf("Failed to get response: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Error("Expected non-empty response body")
	}

	lbFrom := resp.Header.Get("lb-from")
	if lbFrom == "" {
		t.Error("Expected lb-from header")
	}
	t.Logf("response from [%s]", lbFrom)
}

func testDatabaseIntegration(t *testing.T) {
	resp, err := client.Get(apiUrlWithKey())
	if err != nil {
		t.Fatalf("Failed to get response: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
		return
	}

	contentType := resp.Header.Get("Content-Type")
	if !strings.Contains(contentType, "application/json") {
		t.Errorf("Expected JSON content type, got %s", contentType)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Failed to read response body: %v", err)
	}

	var responseValue string

	if err := json.Unmarshal(body, &responseValue); err != nil {
		t.Fatalf("Failed to parse JSON string response: %v. Raw response: %s", err, string(body))
	}

	if responseValue != currentDate {
		t.Errorf("Expected value '%s', got '%s'", currentDate, responseValue)
	} else {
		t.Logf("Successfully retrieved expected value from DB: %s", responseValue)
	}

	t.Logf("Raw response body: %s", string(body))

	lbFrom := resp.Header.Get("lb-from")
	if lbFrom == "" {
		t.Error("Expected lb-from header to confirm load balancer routing")
	} else {
		t.Logf("Request successfully routed through load balancer from server: %s", lbFrom)
	}
}

func testLoadDistribution(t *testing.T) {
	serverHits := make(map[string]int)

	for i := 0; i < 20; i++ {
		resp, err := client.Get(apiUrlWithKey())
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
		resp, err := client.Get(apiUrlWithKey())
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

func testInvalidKey(t *testing.T) {
	invalidKeyUrl := buildUrl(apiPath) + "?key=nonexistent"

	resp, err := client.Get(invalidKeyUrl)
	if err != nil {
		t.Fatalf("Failed to get response for invalid key: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("Expected status 404 for invalid key, got %d", resp.StatusCode)
	}

	t.Logf("Correctly handled invalid key with status: %d", resp.StatusCode)
}

func testMissingKey(t *testing.T) {
	missingKeyUrl := buildUrl(apiPath)

	resp, err := client.Get(missingKeyUrl)
	if err != nil {
		t.Fatalf("Failed to get response for missing key: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("Expected status 400 for missing key, got %d", resp.StatusCode)
	}

	t.Logf("Correctly handled missing key with status: %d", resp.StatusCode)
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
	b.Run("DatabaseLoad", benchmarkDatabaseLoad)
}

func benchmarkSequential(b *testing.B) {
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(apiUrlWithKey())
		if err != nil {
			b.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()
	}
}

func benchmarkParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			resp, err := client.Get(apiUrlWithKey())
			if err != nil {
				b.Fatalf("Request failed: %v", err)
			}
			resp.Body.Close()
		}
	})
}

func benchmarkMultipleEndpoints(b *testing.B) {
	endpoints := []string{
		apiPath + "?key=wholelottago",
		"/api/v1/users?key=wholelottago",
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

func benchmarkDatabaseLoad(b *testing.B) {
	successCount := 0
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := client.Get(apiUrlWithKey())
		if err != nil {
			b.Fatalf("Request failed: %v", err)
		}

		if resp.StatusCode == http.StatusOK {
			successCount++
		}

		resp.Body.Close()
	}

	successRate := float64(successCount) / float64(b.N) * 100
	b.ReportMetric(successRate, "%success")
}

func benchmarkThroughput(b *testing.B) {
	start := time.Now()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resp, err := client.Get(apiUrlWithKey())
		if err != nil {
			b.Fatalf("Request failed: %v", err)
		}
		resp.Body.Close()
	}

	duration := time.Since(start)
	b.ReportMetric(float64(b.N)/duration.Seconds(), "requests/sec")
}
