package dns

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/cluster"
)

// InternalDNSProvider provides an in-memory DNS simulation for testing
type InternalDNSProvider struct {
	config  *cluster.ClusterConfig
	records map[string]string
	mu      sync.RWMutex
}

// NewInternalDNSProvider creates a new internal DNS provider
func NewInternalDNSProvider(config *cluster.ClusterConfig) (*InternalDNSProvider, error) {
	return &InternalDNSProvider{
		config:  config,
		records: make(map[string]string),
	}, nil
}

// UpdateRecord updates a DNS record (simulated)
func (p *InternalDNSProvider) UpdateRecord(ctx context.Context, domain, recordType, value string, ttl int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	key := fmt.Sprintf("%s:%s", domain, recordType)
	p.records[key] = value
	
	log.Printf("[InternalDNS] Updated record: %s %s -> %s (TTL: %d)", 
		domain, recordType, value, ttl)
	
	return nil
}

// DeleteRecord removes a DNS record (simulated)
func (p *InternalDNSProvider) DeleteRecord(ctx context.Context, domain, recordType string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	key := fmt.Sprintf("%s:%s", domain, recordType)
	delete(p.records, key)
	
	log.Printf("[InternalDNS] Deleted record: %s %s", domain, recordType)
	
	return nil
}

// GetRecord retrieves a DNS record (simulated)
func (p *InternalDNSProvider) GetRecord(ctx context.Context, domain, recordType string) (string, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	key := fmt.Sprintf("%s:%s", domain, recordType)
	value, exists := p.records[key]
	if !exists {
		return "", fmt.Errorf("record not found: %s %s", domain, recordType)
	}
	
	log.Printf("[InternalDNS] Retrieved record: %s %s -> %s", domain, recordType, value)
	
	return value, nil
}

// Name returns the provider name
func (p *InternalDNSProvider) Name() string {
	return "internal"
}

// GetAllRecords returns all current records (for debugging)
func (p *InternalDNSProvider) GetAllRecords() map[string]string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	
	result := make(map[string]string)
	for k, v := range p.records {
		result[k] = v
	}
	
	return result
}