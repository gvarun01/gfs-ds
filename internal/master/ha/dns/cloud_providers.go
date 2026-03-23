package dns

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	route53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	"github.com/cloudflare/cloudflare-go"
	
	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master/ha/cluster"
)

// Route53Provider provides AWS Route53 DNS management
type Route53Provider struct {
	config       *cluster.ClusterConfig
	client       *route53.Client
	hostedZoneID string
}

// NewRoute53Provider creates a new Route53 DNS provider
func NewRoute53Provider(config *cluster.ClusterConfig) (*Route53Provider, error) {
	if config.Route53Config == nil {
		return nil, fmt.Errorf("Route53 configuration is required")
	}
	
	if config.Route53Config.HostedZoneID == "" {
		return nil, fmt.Errorf("Route53 hosted_zone_id is required")
	}
	
	// Configure AWS SDK
	var cfg aws.Config
	var err error
	
	if config.Route53Config.UseIAMRole {
		// Use IAM instance role or profile
		if config.Route53Config.AWSProfile != "" {
			cfg, err = awsconfig.LoadDefaultConfig(context.TODO(),
				awsconfig.WithSharedConfigProfile(config.Route53Config.AWSProfile),
				awsconfig.WithRegion(getRegion(config.Route53Config)))
		} else {
			cfg, err = awsconfig.LoadDefaultConfig(context.TODO(),
				awsconfig.WithRegion(getRegion(config.Route53Config)))
		}
	} else {
		// Use explicit credentials
		if config.Route53Config.AccessKeyID == "" || config.Route53Config.SecretAccessKey == "" {
			return nil, fmt.Errorf("Route53 access_key_id and secret_access_key are required when not using IAM role")
		}
		
		cfg, err = awsconfig.LoadDefaultConfig(context.TODO(),
			awsconfig.WithRegion(getRegion(config.Route53Config)),
			awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
				config.Route53Config.AccessKeyID,
				config.Route53Config.SecretAccessKey,
				"")))
	}
	
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %v", err)
	}
	
	client := route53.NewFromConfig(cfg)
	
	// Validate hosted zone access
	if err := validateRoute53Access(context.Background(), client, config.Route53Config.HostedZoneID); err != nil {
		return nil, fmt.Errorf("failed to validate Route53 access: %v", err)
	}
	
	return &Route53Provider{
		config:       config,
		client:       client,
		hostedZoneID: config.Route53Config.HostedZoneID,
	}, nil
}

// UpdateRecord updates a DNS record in Route53
func (p *Route53Provider) UpdateRecord(ctx context.Context, domain, recordType, value string, ttl int) error {
	// Ensure domain ends with dot for Route53
	if !strings.HasSuffix(domain, ".") {
		domain = domain + "."
	}
	
	input := &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(p.hostedZoneID),
		ChangeBatch: &route53types.ChangeBatch{
			Comment: aws.String(fmt.Sprintf("GFS Master DNS update for %s", domain)),
			Changes: []route53types.Change{
				{
					Action: route53types.ChangeActionUpsert,
					ResourceRecordSet: &route53types.ResourceRecordSet{
						Name: aws.String(domain),
						Type: route53types.RRType(recordType),
						TTL:  aws.Int64(int64(ttl)),
						ResourceRecords: []route53types.ResourceRecord{
							{
								Value: aws.String(value),
							},
						},
					},
				},
			},
		},
	}
	
	result, err := p.client.ChangeResourceRecordSets(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to update Route53 record: %v", err)
	}
	
	// Wait for change to propagate
	waiter := route53.NewResourceRecordSetsChangedWaiter(p.client)
	err = waiter.Wait(ctx, &route53.GetChangeInput{
		Id: result.ChangeInfo.Id,
	}, 300*time.Second) // 5 minute timeout
	
	if err != nil {
		return fmt.Errorf("failed to wait for Route53 change propagation: %v", err)
	}
	
	return nil
}

// DeleteRecord removes a DNS record from Route53
func (p *Route53Provider) DeleteRecord(ctx context.Context, domain, recordType string) error {
	// Ensure domain ends with dot for Route53
	if !strings.HasSuffix(domain, ".") {
		domain = domain + "."
	}
	
	// First, get the current record to delete
	listInput := &route53.ListResourceRecordSetsInput{
		HostedZoneId:    aws.String(p.hostedZoneID),
		StartRecordName: aws.String(domain),
		StartRecordType: route53types.RRType(recordType),
	}
	
	listResult, err := p.client.ListResourceRecordSets(ctx, listInput)
	if err != nil {
		return fmt.Errorf("failed to list Route53 records: %v", err)
	}
	
	var recordToDelete *route53types.ResourceRecordSet
	for _, record := range listResult.ResourceRecordSets {
		if *record.Name == domain && string(record.Type) == recordType {
			recordToDelete = &record
			break
		}
	}
	
	if recordToDelete == nil {
		return nil // Record doesn't exist, nothing to delete
	}
	
	// Delete the record
	input := &route53.ChangeResourceRecordSetsInput{
		HostedZoneId: aws.String(p.hostedZoneID),
		ChangeBatch: &route53types.ChangeBatch{
			Comment: aws.String(fmt.Sprintf("GFS Master DNS deletion for %s", domain)),
			Changes: []route53types.Change{
				{
					Action:            route53types.ChangeActionDelete,
					ResourceRecordSet: recordToDelete,
				},
			},
		},
	}
	
	result, err := p.client.ChangeResourceRecordSets(ctx, input)
	if err != nil {
		return fmt.Errorf("failed to delete Route53 record: %v", err)
	}
	
	// Wait for change to propagate
	waiter := route53.NewResourceRecordSetsChangedWaiter(p.client)
	err = waiter.Wait(ctx, &route53.GetChangeInput{
		Id: result.ChangeInfo.Id,
	}, 300*time.Second) // 5 minute timeout
	
	if err != nil {
		return fmt.Errorf("failed to wait for Route53 change propagation: %v", err)
	}
	
	return nil
}

// GetRecord retrieves a DNS record from Route53
func (p *Route53Provider) GetRecord(ctx context.Context, domain, recordType string) (string, error) {
	// Ensure domain ends with dot for Route53
	if !strings.HasSuffix(domain, ".") {
		domain = domain + "."
	}
	
	input := &route53.ListResourceRecordSetsInput{
		HostedZoneId:    aws.String(p.hostedZoneID),
		StartRecordName: aws.String(domain),
		StartRecordType: route53types.RRType(recordType),
	}
	
	result, err := p.client.ListResourceRecordSets(ctx, input)
	if err != nil {
		return "", fmt.Errorf("failed to get Route53 record: %v", err)
	}
	
	for _, record := range result.ResourceRecordSets {
		if *record.Name == domain && string(record.Type) == recordType {
			if len(record.ResourceRecords) > 0 {
				return *record.ResourceRecords[0].Value, nil
			}
		}
	}
	
	return "", fmt.Errorf("record not found")
}

// Name returns the provider name
func (p *Route53Provider) Name() string {
	return "route53"
}

// CloudflareProvider provides Cloudflare DNS management
type CloudflareProvider struct {
	config *cluster.ClusterConfig
	api    *cloudflare.API
	zoneID string
}

// NewCloudflareProvider creates a new Cloudflare DNS provider
func NewCloudflareProvider(config *cluster.ClusterConfig) (*CloudflareProvider, error) {
	if config.CloudflareConfig == nil {
		return nil, fmt.Errorf("Cloudflare configuration is required")
	}
	
	if config.CloudflareConfig.ZoneID == "" {
		return nil, fmt.Errorf("Cloudflare zone_id is required")
	}
	
	var api *cloudflare.API
	var err error
	
	if config.CloudflareConfig.UseAPIToken {
		// Use API Token (recommended)
		if config.CloudflareConfig.APIToken == "" {
			return nil, fmt.Errorf("Cloudflare api_token is required when use_api_token is true")
		}
		
		api, err = cloudflare.NewWithAPIToken(config.CloudflareConfig.APIToken)
	} else {
		// Use API Key + Email (legacy)
		if config.CloudflareConfig.APIKey == "" || config.CloudflareConfig.Email == "" {
			return nil, fmt.Errorf("Cloudflare api_key and email are required when use_api_token is false")
		}
		
		api, err = cloudflare.New(config.CloudflareConfig.APIKey, config.CloudflareConfig.Email)
	}
	
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloudflare API client: %v", err)
	}
	
	// Validate zone access
	if err := validateCloudflareAccess(context.Background(), api, config.CloudflareConfig.ZoneID); err != nil {
		return nil, fmt.Errorf("failed to validate Cloudflare access: %v", err)
	}
	
	return &CloudflareProvider{
		config: config,
		api:    api,
		zoneID: config.CloudflareConfig.ZoneID,
	}, nil
}

// UpdateRecord updates a DNS record in Cloudflare
func (p *CloudflareProvider) UpdateRecord(ctx context.Context, domain, recordType, value string, ttl int) error {
	// Check if record already exists
	records, _, err := p.api.ListDNSRecords(ctx, cloudflare.ZoneIdentifier(p.zoneID), cloudflare.ListDNSRecordsParams{
		Name: domain,
		Type: recordType,
	})
	if err != nil {
		return fmt.Errorf("failed to list Cloudflare DNS records: %v", err)
	}
	
	recordParams := cloudflare.CreateDNSRecordParams{
		Type:    recordType,
		Name:    domain,
		Content: value,
		TTL:     ttl,
		Comment: "GFS Master DNS record",
	}
	
	if len(records) > 0 {
		// Update existing record
		updateParams := cloudflare.UpdateDNSRecordParams{
			Type:    recordType,
			Name:    domain,
			Content: value,
			TTL:     ttl,
			Comment: "GFS Master DNS record - updated",
		}
		
		_, err = p.api.UpdateDNSRecord(ctx, cloudflare.ZoneIdentifier(p.zoneID), cloudflare.DNSRecordIdentifier(records[0].ID), updateParams)
		if err != nil {
			return fmt.Errorf("failed to update Cloudflare DNS record: %v", err)
		}
	} else {
		// Create new record
		_, err = p.api.CreateDNSRecord(ctx, cloudflare.ZoneIdentifier(p.zoneID), recordParams)
		if err != nil {
			return fmt.Errorf("failed to create Cloudflare DNS record: %v", err)
		}
	}
	
	return nil
}

// DeleteRecord removes a DNS record from Cloudflare
func (p *CloudflareProvider) DeleteRecord(ctx context.Context, domain, recordType string) error {
	// Find the record to delete
	records, _, err := p.api.ListDNSRecords(ctx, cloudflare.ZoneIdentifier(p.zoneID), cloudflare.ListDNSRecordsParams{
		Name: domain,
		Type: recordType,
	})
	if err != nil {
		return fmt.Errorf("failed to list Cloudflare DNS records: %v", err)
	}
	
	if len(records) == 0 {
		return nil // Record doesn't exist, nothing to delete
	}
	
	// Delete the record
	err = p.api.DeleteDNSRecord(ctx, cloudflare.ZoneIdentifier(p.zoneID), cloudflare.DNSRecordIdentifier(records[0].ID))
	if err != nil {
		return fmt.Errorf("failed to delete Cloudflare DNS record: %v", err)
	}
	
	return nil
}

// GetRecord retrieves a DNS record from Cloudflare
func (p *CloudflareProvider) GetRecord(ctx context.Context, domain, recordType string) (string, error) {
	records, _, err := p.api.ListDNSRecords(ctx, cloudflare.ZoneIdentifier(p.zoneID), cloudflare.ListDNSRecordsParams{
		Name: domain,
		Type: recordType,
	})
	if err != nil {
		return "", fmt.Errorf("failed to get Cloudflare DNS record: %v", err)
	}
	
	if len(records) == 0 {
		return "", fmt.Errorf("record not found")
	}
	
	return records[0].Content, nil
}

// Name returns the provider name
func (p *CloudflareProvider) Name() string {
	return "cloudflare"
}

// Helper functions

// getRegion returns the AWS region, defaulting to us-east-1
func getRegion(config *cluster.Route53Config) string {
	if config.AWSRegion != "" {
		return config.AWSRegion
	}
	return "us-east-1"
}

// validateRoute53Access validates that the client can access the hosted zone
func validateRoute53Access(ctx context.Context, client *route53.Client, hostedZoneID string) error {
	input := &route53.GetHostedZoneInput{
		Id: aws.String(hostedZoneID),
	}
	
	_, err := client.GetHostedZone(ctx, input)
	if err != nil {
		return fmt.Errorf("cannot access hosted zone %s: %v", hostedZoneID, err)
	}
	
	return nil
}

// validateCloudflareAccess validates that the API client can access the zone
func validateCloudflareAccess(ctx context.Context, api *cloudflare.API, zoneID string) error {
	_, err := api.ZoneDetails(ctx, zoneID)
	if err != nil {
		return fmt.Errorf("cannot access zone %s: %v", zoneID, err)
	}
	
	return nil
}