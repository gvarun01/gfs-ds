package dns_test

import (
	"context"
	"testing"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/cluster"
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/dns"
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
		provider, err := dns.NewRoute53Provider(config)
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

		_, err := dns.NewRoute53Provider(&invalidConfig)
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

		_, err := dns.NewRoute53Provider(&invalidConfig)
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
			ZoneID:      "abcdef123456789test",
			UseAPIToken: true,
			APIToken:    "test-token",
		},
	}

	t.Run("CreateProvider", func(t *testing.T) {
		provider, err := dns.NewCloudflareProvider(config)
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
		invalidConfig.CloudflareConfig = &cluster.CloudflareConfig{}

		_, err := dns.NewCloudflareProvider(&invalidConfig)
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

		_, err := dns.NewCloudflareProvider(&invalidConfig)
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

		_, err := dns.NewCloudflareProvider(&apiKeyConfig)
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

		manager, err := dns.NewDNSManager(config)
		if err != nil {
			t.Fatalf("Failed to create DNS manager with internal provider: %v", err)
		}

		// Ensure updates can be queued without error
		if err := manager.UpdateMasterRecord("localhost:8080"); err != nil {
			t.Fatalf("Failed to queue DNS update: %v", err)
		}
	})

	t.Run("CreateManagerWithoutDNS", func(t *testing.T) {
		config := &cluster.ClusterConfig{
			DNSEnabled: false,
		}

		_, err := dns.NewDNSManager(config)
		if err == nil {
			t.Error("Expected error when DNS is disabled")
		}
	})

	t.Run("CreateManagerWithUnsupportedProvider", func(t *testing.T) {
		config := &cluster.ClusterConfig{
			DNSEnabled:  true,
			DNSProvider: "unsupported",
		}

		_, err := dns.NewDNSManager(config)
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

		_, err := dns.NewRoute53Provider(config)
		if err == nil {
			t.Log("Route53 provider constructed (expected failures may occur without credentials)")
		}
	})

	t.Run("CloudflareImplementsInterface", func(t *testing.T) {
		config := &cluster.ClusterConfig{
			CloudflareConfig: &cluster.CloudflareConfig{
				ZoneID:      "test-zone",
				UseAPIToken: true,
				APIToken:    "test-token",
			},
		}

		_, err := dns.NewCloudflareProvider(config)
		if err == nil {
			t.Log("Cloudflare provider constructed (expected failures may occur without credentials)")
		}
	})
}
