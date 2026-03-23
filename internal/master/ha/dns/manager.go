package dns

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/cluster"
)

// DNSProvider defines the interface for DNS providers
type DNSProvider interface {
	// UpdateRecord updates a DNS record to point to the specified address
	UpdateRecord(ctx context.Context, domain, recordType, value string, ttl int) error
	
	// DeleteRecord removes a DNS record
	DeleteRecord(ctx context.Context, domain, recordType string) error
	
	// GetRecord retrieves the current DNS record value
	GetRecord(ctx context.Context, domain, recordType string) (string, error)
	
	// Name returns the provider name
	Name() string
}

// DNSManager manages DNS records for master failover
type DNSManager struct {
	config   *cluster.ClusterConfig
	provider DNSProvider
	
	// Current state
	currentMaster string
	masterDomain  string
	
	// Synchronization
	mu sync.RWMutex
	
	// Background updater
	updateCh   chan DNSUpdate
	shutdownCh chan struct{}
	running    bool
	runMu      sync.Mutex
}

// DNSUpdate represents a DNS update request
type DNSUpdate struct {
	Operation string // "set", "delete"
	Domain    string
	Value     string
	TTL       int
}

// NewDNSManager creates a new DNS manager
func NewDNSManager(config *cluster.ClusterConfig) (*DNSManager, error) {
	if !config.DNSEnabled {
		return nil, fmt.Errorf("DNS management not enabled")
	}
	
	// Create provider based on configuration
	provider, err := createProvider(config.DNSProvider, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create DNS provider: %v", err)
	}
	
	manager := &DNSManager{
		config:       config,
		provider:     provider,
		masterDomain: fmt.Sprintf("master.%s", config.DNSDomain),
		updateCh:     make(chan DNSUpdate, 10),
		shutdownCh:   make(chan struct{}),
	}
	
	return manager, nil
}

// createProvider creates a DNS provider based on the provider type
func createProvider(providerType string, config *cluster.ClusterConfig) (DNSProvider, error) {
	switch providerType {
	case "internal":
		return NewInternalDNSProvider(config)
	case "route53":
		return NewRoute53Provider(config)
	case "cloudflare":
		return NewCloudflareProvider(config)
	default:
		return nil, fmt.Errorf("unsupported DNS provider: %s", providerType)
	}
}

// Start begins DNS management operations
func (dm *DNSManager) Start(ctx context.Context) error {
	dm.runMu.Lock()
	defer dm.runMu.Unlock()
	
	if dm.running {
		return fmt.Errorf("DNS manager already running")
	}
	
	log.Printf("Starting DNS manager with provider: %s", dm.provider.Name())
	
	// Start background updater
	go dm.runUpdater(ctx)
	
	dm.running = true
	log.Printf("DNS manager started, managing domain: %s", dm.masterDomain)
	
	return nil
}

// Stop gracefully shuts down DNS management
func (dm *DNSManager) Stop() error {
	dm.runMu.Lock()
	defer dm.runMu.Unlock()
	
	if !dm.running {
		return nil
	}
	
	log.Printf("Stopping DNS manager")
	close(dm.shutdownCh)
	dm.running = false
	
	return nil
}

// UpdateMasterRecord updates the master DNS record to point to the active master
func (dm *DNSManager) UpdateMasterRecord(masterAddr string) error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	
	if masterAddr == dm.currentMaster {
		log.Printf("DNS record already points to %s, skipping update", masterAddr)
		return nil
	}
	
	log.Printf("Updating master DNS record from %s to %s", dm.currentMaster, masterAddr)
	
	// Extract IP/hostname from address (remove port if present)
	host := extractHost(masterAddr)
	
	update := DNSUpdate{
		Operation: "set",
		Domain:    dm.masterDomain,
		Value:     host,
		TTL:       int(dm.config.DNSTTL.Seconds()),
	}
	
	select {
	case dm.updateCh <- update:
		dm.currentMaster = masterAddr
		log.Printf("Queued DNS update for master record: %s -> %s", dm.masterDomain, host)
		return nil
	default:
		return fmt.Errorf("DNS update queue is full")
	}
}

// DeleteMasterRecord removes the master DNS record
func (dm *DNSManager) DeleteMasterRecord() error {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	
	if dm.currentMaster == "" {
		return nil // Nothing to delete
	}
	
	log.Printf("Deleting master DNS record for %s", dm.masterDomain)
	
	update := DNSUpdate{
		Operation: "delete",
		Domain:    dm.masterDomain,
		Value:     "",
		TTL:       0,
	}
	
	select {
	case dm.updateCh <- update:
		dm.currentMaster = ""
		log.Printf("Queued DNS deletion for master record: %s", dm.masterDomain)
		return nil
	default:
		return fmt.Errorf("DNS update queue is full")
	}
}

// GetCurrentMaster returns the current master address according to DNS
func (dm *DNSManager) GetCurrentMaster() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	value, err := dm.provider.GetRecord(ctx, dm.masterDomain, "A")
	if err != nil {
		return "", fmt.Errorf("failed to get DNS record: %v", err)
	}
	
	return value, nil
}

// runUpdater processes DNS updates in the background
func (dm *DNSManager) runUpdater(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-dm.shutdownCh:
			return
		case update := <-dm.updateCh:
			dm.processUpdate(ctx, update)
		}
	}
}

// processUpdate processes a single DNS update
func (dm *DNSManager) processUpdate(ctx context.Context, update DNSUpdate) {
	log.Printf("Processing DNS update: %s %s -> %s (TTL: %d)", 
		update.Operation, update.Domain, update.Value, update.TTL)
	
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	
	var err error
	switch update.Operation {
	case "set":
		err = dm.provider.UpdateRecord(ctx, update.Domain, "A", update.Value, update.TTL)
	case "delete":
		err = dm.provider.DeleteRecord(ctx, update.Domain, "A")
	default:
		log.Printf("Unknown DNS operation: %s", update.Operation)
		return
	}
	
	if err != nil {
		log.Printf("DNS update failed: %v", err)
		// Could implement retry logic here
	} else {
		log.Printf("DNS update successful: %s %s -> %s", 
			update.Operation, update.Domain, update.Value)
	}
}

// extractHost extracts the hostname/IP from an address (removes port)
func extractHost(addr string) string {
	// Simple implementation - in production would use net.SplitHostPort
	if idx := lastIndex(addr, ':'); idx != -1 {
		return addr[:idx]
	}
	return addr
}

// lastIndex finds the last occurrence of a character in a string
func lastIndex(s string, c rune) int {
	for i := len(s) - 1; i >= 0; i-- {
		if rune(s[i]) == c {
			return i
		}
	}
	return -1
}