package dns

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/cluster"
)

// TestRoute53Provider tests Route53 DNS provider functionality
func TestRoute53Provider(t *testing.T) {
	// Skip if no AWS credentials available
	if testing.Short() {
		t.Skip("Skipping Route53 integration test in short mode")
	}

	config := &cluster.ClusterConfig{
		DNSEnabled:  true,
		DNSDomain:   "test.example.com",
		DNSTTL:      30 * time.Second,
		DNSProvider: "route53",
		Route53Config: &cluster.Route53Config{
			HostedZoneID: "Z123456789TEST", // Test zone ID
			AWSRegion:    "us-east-1",
			UseIAMRole:   true,
		},
	}

	t.Run("CreateProvider", func(t *testing.T) {
		provider, err := NewRoute53Provider(config)
		if err != nil {
			// Expected to fail with test credentials
			t.Logf("Expected error with test credentials: %v", err)
			return
		}
		
		if provider.Name() != "route53" {
			t.Errorf("Expected provider name 'route53', got %s", provider.Name())
		}
	})

	t.Run("ValidateConfig", func(t *testing.T) {
		// Test missing hosted zone ID
		invalidConfig := *config
		invalidConfig.Route53Config = &cluster.Route53Config{}
		
		_, err := NewRoute53Provider(&invalidConfig)
		if err == nil {
			t.Error("Expected error for missing hosted zone ID")
		}
	})

	t.Run("ValidateCredentials", func(t *testing.T) {
		// Test invalid credentials configuration
		invalidConfig := *config
		invalidConfig.Route53Config = &cluster.Route53Config{
			HostedZoneID: "Z123456789TEST",
			UseIAMRole:   false,
			// Missing access keys
		}
		
		_, err := NewRoute53Provider(&invalidConfig)
		if err == nil {
			t.Error("Expected error for missing credentials")
		}
	})
}

// TestCloudflareProvider tests Cloudflare DNS provider functionality  
func TestCloudflareProvider(t *testing.T) {
	// Skip if no Cloudflare credentials available
	if testing.Short() {
		t.Skip("Skipping Cloudflare integration test in short mode")
	}

	config := &cluster.ClusterConfig{
		DNSEnabled:  true,
		DNSDomain:   "test.example.com", 
		DNSTTL:      30 * time.Second,
		DNSProvider: "cloudflare",
		CloudflareConfig: &cluster.CloudflareConfig{
			ZoneID:       "abcdef123456789test",
			UseAPIToken:  true,
			APIToken:     "test-token",
		},
	}

	t.Run("CreateProvider", func(t *testing.T) {
		provider, err := NewCloudflareProvider(config)
		if err != nil {
			// Expected to fail with test credentials
			t.Logf("Expected error with test credentials: %v", err)
			return
		}
		
		if provider.Name() != "cloudflare" {
			t.Errorf("Expected provider name 'cloudflare', got %s", provider.Name())
		}
	})

	t.Run("ValidateConfig", func(t *testing.T) {
		// Test missing zone ID
		invalidConfig := *config
		invalidConfig.CloudflareConfig = &cluster.CloudflareConfig{
			UseAPIToken: true,
			APIToken:    "test-token",
		}
		
		_, err := NewCloudflareProvider(&invalidConfig)
		if err == nil {
			t.Error("Expected error for missing zone ID")
		}
	})

	t.Run("ValidateAPITokenConfig", func(t *testing.T) {
		// Test missing API token
		invalidConfig := *config
		invalidConfig.CloudflareConfig = &cluster.CloudflareConfig{
			ZoneID:      "test-zone",
			UseAPIToken: true,
			// Missing APIToken
		}
		
		_, err := NewCloudflareProvider(&invalidConfig)
		if err == nil {
			t.Error("Expected error for missing API token")
		}
	})

	t.Run("ValidateAPIKeyConfig", func(t *testing.T) {
		// Test API key configuration
		apiKeyConfig := *config
		apiKeyConfig.CloudflareConfig = &cluster.CloudflareConfig{
			ZoneID:      "test-zone", 
			UseAPIToken: false,
			APIKey:      "test-key",
			// Missing Email
		}
		
		_, err := NewCloudflareProvider(&apiKeyConfig)
		if err == nil {
			t.Error("Expected error for missing email with API key")
		}
	})
}

// TestDNSManager tests the DNS manager with different providers
func TestDNSManager(t *testing.T) {
	t.Run("CreateManagerWithInternalProvider", func(t *testing.T) {
		config := &cluster.ClusterConfig{
			DNSEnabled:  true,
			DNSDomain:   "test.local",
			DNSTTL:      30 * time.Second,
			DNSProvider: "internal",
		}

		manager, err := NewDNSManager(config)
		if err != nil {
			t.Fatalf("Failed to create DNS manager with internal provider: %v", err)
		}

		if manager.provider.Name() != "internal" {
			t.Errorf("Expected internal provider, got %s", manager.provider.Name())
		}
	})

	t.Run("CreateManagerWithoutDNS", func(t *testing.T) {
		config := &cluster.ClusterConfig{
			DNSEnabled: false,
		}

		_, err := NewDNSManager(config)
		if err == nil {
			t.Error("Expected error when DNS is disabled")
		}
	})

	t.Run("CreateManagerWithUnsupportedProvider", func(t *testing.T) {
		config := &cluster.ClusterConfig{
			DNSEnabled:  true,
			DNSProvider: "unsupported",
		}

		_, err := NewDNSManager(config)
		if err == nil {
			t.Error("Expected error for unsupported DNS provider")
		}
	})
}

// TestCloudDNSProviderInterface tests that both providers implement the interface correctly
func TestCloudDNSProviderInterface(t *testing.T) {
	t.Run("Route53ImplementsInterface", func(t *testing.T) {
		config := &cluster.ClusterConfig{
			Route53Config: &cluster.Route53Config{
				HostedZoneID: "test",
				UseIAMRole:   true,
			},
		}
		
		// This will fail but we're testing the interface compliance
		provider, _ := NewRoute53Provider(config)
		if provider != nil {
			var _ DNSProvider = provider
			t.Log("Route53Provider correctly implements DNSProvider interface")
		}
	})

	t.Run("CloudflareImplementsInterface", func(t *testing.T) {
		config := &cluster.ClusterConfig{
			CloudflareConfig: &cluster.CloudflareConfig{
				ZoneID:      "test",
				UseAPIToken: true,
				APIToken:    "test",
			},
		}
		
		// This will fail but we're testing the interface compliance
		provider, _ := NewCloudflareProvider(config)
		if provider != nil {
			var _ DNSProvider = provider
			t.Log("CloudflareProvider correctly implements DNSProvider interface")
		}
	})
}

// BenchmarkDNSOperations benchmarks DNS provider operations
func BenchmarkDNSOperations(b *testing.B) {
	if testing.Short() {
		b.Skip("Skipping benchmark in short mode")
	}

	config := &cluster.ClusterConfig{
		DNSEnabled:  true,
		DNSDomain:   "benchmark.test.local",
		DNSTTL:      30 * time.Second,
		DNSProvider: "internal",
	}

	manager, err := NewDNSManager(config)
	if err != nil {
		b.Fatalf("Failed to create DNS manager: %v", err)
	}

	ctx := context.Background()

	b.Run("UpdateRecord", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			err := manager.provider.UpdateRecord(ctx, 
				fmt.Sprintf("test-%d.%s", i, config.DNSDomain),
				"A", "192.168.1.1", 30)
			if err != nil {
				b.Errorf("UpdateRecord failed: %v", err)
			}
		}
	})

	b.Run("GetRecord", func(b *testing.B) {
		// Setup a record first
		testDomain := fmt.Sprintf("get-test.%s", config.DNSDomain)
		manager.provider.UpdateRecord(ctx, testDomain, "A", "192.168.1.1", 30)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := manager.provider.GetRecord(ctx, testDomain, "A")
			if err != nil {
				b.Errorf("GetRecord failed: %v", err)
			}
		}
	})
}

// TestDNSProviderFailover tests DNS operations during simulated failures
func TestDNSProviderFailover(t *testing.T) {
	config := &cluster.ClusterConfig{
		DNSEnabled:  true,
		DNSDomain:   "failover.test.local",
		DNSTTL:      30 * time.Second, 
		DNSProvider: "internal",
	}

	manager, err := NewDNSManager(config)
	if err != nil {
		t.Fatalf("Failed to create DNS manager: %v", err)
	}

	ctx := context.Background()
	err = manager.Start(ctx)
	if err != nil {
		t.Fatalf("Failed to start DNS manager: %v", err)
	}
	defer manager.Stop()

	t.Run("SequentialMasterUpdates", func(t *testing.T) {
		// Simulate multiple master failovers
		masters := []string{
			"master1.example.com",
			"master2.example.com", 
			"master3.example.com",
		}

		for i, master := range masters {
			err := manager.UpdateMasterRecord(master)
			if err != nil {
				t.Errorf("Failed to update master record %d: %v", i, err)
			}

			// Give some time for async update
			time.Sleep(100 * time.Millisecond)

			// Verify the update
			current, err := manager.GetCurrentMaster()
			if err != nil {
				t.Errorf("Failed to get current master: %v", err)
			}

			expectedHost := extractHost(master)
			if current != expectedHost {
				t.Errorf("Expected master %s, got %s", expectedHost, current)
			}
		}
	})

	t.Run("DeleteAndRecreate", func(t *testing.T) {
		// Update master record
		testMaster := "delete-test.example.com"
		err := manager.UpdateMasterRecord(testMaster)
		if err != nil {
			t.Errorf("Failed to set master record: %v", err)
		}

		// Delete master record
		err = manager.DeleteMasterRecord()
		if err != nil {
			t.Errorf("Failed to delete master record: %v", err)
		}

		// Recreate master record
		newMaster := "recreate-test.example.com"
		err = manager.UpdateMasterRecord(newMaster)
		if err != nil {
			t.Errorf("Failed to recreate master record: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		current, err := manager.GetCurrentMaster()
		if err != nil {
			t.Errorf("Failed to get recreated master: %v", err)
		}

		expectedHost := extractHost(newMaster)
		if current != expectedHost {
			t.Errorf("Expected recreated master %s, got %s", expectedHost, current)
		}
	})
}

// Helper function to extract host from address (testing the actual utility)
func TestExtractHost(t *testing.T) {
	testCases := []struct {
		input    string
		expected string
	}{
		{"192.168.1.1:8080", "192.168.1.1"},
		{"master.example.com:50051", "master.example.com"},
		{"localhost:9090", "localhost"},
		{"192.168.1.1", "192.168.1.1"},
		{"master.example.com", "master.example.com"},
		{"[::1]:8080", "[::1]"},
	}

	for _, tc := range testCases {
		result := extractHost(tc.input)
		if result != tc.expected {
			t.Errorf("extractHost(%s) = %s, expected %s", tc.input, result, tc.expected)
		}
	}
}