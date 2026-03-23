package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/chunkserver"
	"github.com/Mit-Vin/GFS-Distributed-Systems/pkg/configpath"
)

func main() {
	// Parse command-line arguments
	port := flag.Int("port", 8001, "Port number to run the chunk server on")
	configFlag := flag.String("config", "", "Path to chunkserver configuration file")
	flag.Parse()

	// Validate the provided port number
	if *port <= 0 || *port > 65535 {
		log.Fatalf("Invalid port number: %d. Please provide a port between 1 and 65535.\n", *port)
	}

	// Generate server ID and address
	serverID := fmt.Sprintf("server-%d", *port)
	address := fmt.Sprintf("localhost:%d", *port)
	log.Printf("Initializing chunk server with ID: %s at address: %s\n", serverID, address)

	configPath, err := configpath.Resolve(
		*configFlag,
		"GFS_CHUNKSERVER_CONFIG",
		[]string{
			"configs/chunkserver-config.yml",
			"../../configs/chunkserver-config.yml",
		},
	)
	if err != nil {
		log.Fatalf("Failed to resolve chunkserver configuration file: %v\n", err)
	}

	// Load server configuration
	config, err := chunkserver.LoadConfig(configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration file: %v\n", err)
	}

	// Verify if the port is available
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Port %d is not available: %v\n", *port, err)
	}
	// The listener is not used further; defer closing to clean up resources
	listener.Close()
	log.Printf("Port %d is available. Proceeding to initialize the chunk server.\n", *port)

	// Create the chunk server instance
	cs, err := chunkserver.NewChunkServer(serverID, address, config)
	if err != nil {
		log.Fatalf("Failed to create chunk server: %v\n", err)
	}

	// Start the chunk server
	log.Printf("Starting chunk server with ID: %s\n", serverID)
	if err := cs.Start(); err != nil {
		log.Fatalf("Failed to start chunk server: %v\n", err)
	}

	// Keep the program running indefinitely
	log.Printf("Chunk server with ID %s is now running.\n", serverID)
	select {}
}
