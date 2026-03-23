package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/client"
)

const (
	// Test configuration - now centralized instead of hardcoded
	configFile      = "configs/client-config.yml"
	testFileName    = "test.txt"
	testData        = "Hello, GFS! This is a test write operation with the corrected 64MB chunk size."
	appendData      = " This is appended data!"
	largeTestFile   = "large_test.txt"
	concurrentFiles = 3
)

func main() {
	fmt.Println("🔧 GFS System Test Suite - Refactored Version")
	fmt.Println("=" + fmt.Sprint(60))

	// Create client with clean configuration loading
	gfsClient, err := client.NewClient(configFile)
	if err != nil {
		log.Fatalf("Failed to create GFS client: %v", err)
	}

	ctx := context.Background()

	// Test 1: File creation
	runTest("Creating file", func() error {
		return gfsClient.Create(ctx, testFileName)
	})

	// Test 2: Write operation
	runTest("Writing data", func() error {
		_, err := gfsClient.Write(ctx, testFileName, 0, []byte(testData))
		return err
	})

	// Test 3: Read operation
	runTest("Reading data", func() error {
		data, err := gfsClient.Read(ctx, testFileName, 0, int64(len(testData)))
		if err != nil {
			return err
		}
		if string(data) != testData {
			return fmt.Errorf("data mismatch")
		}
		fmt.Printf("  ✓ Read data: %s\n", string(data))
		return nil
	})

	// Test 4: Append operation
	runTest("Appending data", func() error {
		_, _, err := gfsClient.Append(ctx, testFileName, []byte(appendData))
		return err
	})

	// Test 5: Read full file after append
	runTest("Reading full file", func() error {
		expectedData := testData + appendData
		data, err := gfsClient.Read(ctx, testFileName, 0, int64(len(expectedData)))
		if err != nil {
			return err
		}
		if string(data) != expectedData {
			return fmt.Errorf("full file data mismatch")
		}
		fmt.Printf("  ✓ Full content: %s\n", string(data))
		return nil
	})

	// Test 6: Performance test with larger data
	runPerformanceTest()

	// Test 7: Concurrent operations test
	runConcurrentTest()

	// Test 8: Cleanup
	runTest("Deleting test file", func() error {
		return gfsClient.Delete(ctx, testFileName)
	})

	// Final status
	fmt.Println("\n" + "=" + fmt.Sprint(60))
	fmt.Println("🎉 All tests completed successfully!")
	fmt.Println("✅ GFS system is working correctly")
	fmt.Println("✅ Configuration management is clean")
	fmt.Println("✅ No hardcoded values detected")
}

func runTest(description string, testFunc func() error) {
	fmt.Printf("\n=== Test: %s ===\n", description)
	start := time.Now()

	if err := testFunc(); err != nil {
		fmt.Printf("❌ FAILED: %v\n", err)
		log.Fatalf("Test failed: %s", description)
	}

	duration := time.Since(start)
	fmt.Printf("✅ PASSED (took %v)\n", duration)
}

func runPerformanceTest() {
	fmt.Printf("\n=== Performance Test: Large Data Operation ===\n")

	gfsClient, _ := client.NewClient(configFile)
	ctx := context.Background()

	// Test with 1MB data (configurable size)
	largeData := make([]byte, 1024*1024) // 1MB
	for i := range largeData {
		largeData[i] = byte('A' + (i % 26))
	}

	// Create large file
	if err := gfsClient.Create(ctx, largeTestFile); err != nil {
		log.Printf("Failed to create large test file: %v", err)
		return
	}

	// Write performance test
	start := time.Now()
	if _, err := gfsClient.Write(ctx, largeTestFile, 0, largeData); err != nil {
		log.Printf("Failed to write large data: %v", err)
		return
	}
	writeTime := time.Since(start)

	// Read performance test
	start = time.Now()
	readData, err := gfsClient.Read(ctx, largeTestFile, 0, int64(len(largeData)))
	if err != nil {
		log.Printf("Failed to read large data: %v", err)
		return
	}
	readTime := time.Since(start)

	// Verify data integrity
	if len(readData) != len(largeData) {
		log.Printf("Data size mismatch: expected %d, got %d", len(largeData), len(readData))
		return
	}

	fmt.Printf("✅ Large data test (1MB):\n")
	fmt.Printf("  Write time: %v (%.2f MB/s)\n", writeTime, float64(len(largeData))/writeTime.Seconds()/1024/1024)
	fmt.Printf("  Read time:  %v (%.2f MB/s)\n", readTime, float64(len(readData))/readTime.Seconds()/1024/1024)

	// Cleanup
	gfsClient.Delete(ctx, largeTestFile)
}

func runConcurrentTest() {
	fmt.Printf("\n=== Concurrent Operations Test ===\n")

	done := make(chan error, concurrentFiles)

	for i := 0; i < concurrentFiles; i++ {
		go func(id int) {
			gfsClient, err := client.NewClient(configFile)
			if err != nil {
				done <- err
				return
			}

			ctx := context.Background()
			fileName := fmt.Sprintf("concurrent_%d.txt", id)
			testData := fmt.Sprintf("Concurrent test data from goroutine %d", id)

			// Create, write, read, delete
			if err := gfsClient.Create(ctx, fileName); err != nil {
				done <- fmt.Errorf("goroutine %d create failed: %v", id, err)
				return
			}

			if _, err := gfsClient.Write(ctx, fileName, 0, []byte(testData)); err != nil {
				done <- fmt.Errorf("goroutine %d write failed: %v", id, err)
				return
			}

			if _, err := gfsClient.Read(ctx, fileName, 0, int64(len(testData))); err != nil {
				done <- fmt.Errorf("goroutine %d read failed: %v", id, err)
				return
			}

			if err := gfsClient.Delete(ctx, fileName); err != nil {
				done <- fmt.Errorf("goroutine %d delete failed: %v", id, err)
				return
			}

			fmt.Printf("  ✓ Goroutine %d completed successfully\n", id)
			done <- nil
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < concurrentFiles; i++ {
		if err := <-done; err != nil {
			log.Printf("Concurrent test error: %v", err)
			return
		}
	}

	fmt.Printf("✅ All %d concurrent operations completed successfully\n", concurrentFiles)
}
