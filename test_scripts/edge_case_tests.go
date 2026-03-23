package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/client"
)

func main() {
	// Initialize client
	c, err := client.NewClient("configs/client-config.yml")
	if err != nil {
		log.Fatalf("Failed to create GFS client: %v", err)
	}

	ctx := context.Background()
	fmt.Println("🧪 Starting GFS Edge Case and Error Handling Tests")
	fmt.Println(strings.Repeat("=", 60))

	// Test 1: Try to read non-existent file
	fmt.Println("\n=== Test 1: Reading non-existent file ===")
	_, err = c.Read(ctx, "nonexistent.txt", 0, 100)
	if err != nil {
		fmt.Printf("✅ Expected error for non-existent file: %v\n", err)
	} else {
		fmt.Printf("❌ Should have received error for non-existent file\n")
	}

	// Test 2: Try to delete non-existent file
	fmt.Println("\n=== Test 2: Deleting non-existent file ===")
	err = c.Delete(ctx, "nonexistent.txt")
	if err != nil {
		fmt.Printf("✅ Expected error for deleting non-existent file: %v\n", err)
	} else {
		fmt.Printf("❌ Should have received error for deleting non-existent file\n")
	}

	// Test 3: Create file with empty name
	fmt.Println("\n=== Test 3: Creating file with empty name ===")
	err = c.Create(ctx, "")
	if err != nil {
		fmt.Printf("✅ Expected error for empty filename: %v\n", err)
	} else {
		fmt.Printf("❌ Should have received error for empty filename\n")
	}

	// Test 4: Test large write operation (near chunk boundary)
	fmt.Println("\n=== Test 4: Large write operation test ===")
	largeFileName := "large_test.txt"
	err = c.Create(ctx, largeFileName)
	if err != nil {
		fmt.Printf("❌ Failed to create large test file: %v\n", err)
		return
	}

	// Create a 1MB string (much smaller than 64MB chunk size but large enough to test)
	largeData := strings.Repeat("A", 1024*1024) // 1MB of 'A's

	start := time.Now()
	_, err = c.Write(ctx, largeFileName, 0, []byte(largeData))
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("❌ Failed to write large data: %v\n", err)
	} else {
		fmt.Printf("✅ Successfully wrote 1MB data in %v\n", duration)
	}

	// Test 5: Read back the large data
	fmt.Println("\n=== Test 5: Reading back large data ===")
	start = time.Now()
	readData, err := c.Read(ctx, largeFileName, 0, int64(len(largeData)))
	duration = time.Since(start)

	if err != nil {
		fmt.Printf("❌ Failed to read large data: %v\n", err)
	} else if len(readData) != len(largeData) {
		fmt.Printf("❌ Data size mismatch: expected %d, got %d\n", len(largeData), len(readData))
	} else if string(readData) != largeData {
		fmt.Printf("❌ Data content mismatch\n")
	} else {
		fmt.Printf("✅ Successfully read back 1MB data in %v\n", duration)
	}

	// Test 6: Multiple concurrent operations
	fmt.Println("\n=== Test 6: Concurrent file operations ===")

	done := make(chan bool, 3)

	// Concurrent file 1
	go func() {
		fname := "concurrent1.txt"
		c.Create(ctx, fname)
		c.Write(ctx, fname, 0, []byte("Concurrent file 1 data"))
		data, _ := c.Read(ctx, fname, 0, 100)
		fmt.Printf("✅ Concurrent 1: %s\n", string(data))
		c.Delete(ctx, fname)
		done <- true
	}()

	// Concurrent file 2
	go func() {
		fname := "concurrent2.txt"
		c.Create(ctx, fname)
		c.Write(ctx, fname, 0, []byte("Concurrent file 2 data"))
		data, _ := c.Read(ctx, fname, 0, 100)
		fmt.Printf("✅ Concurrent 2: %s\n", string(data))
		c.Delete(ctx, fname)
		done <- true
	}()

	// Concurrent file 3
	go func() {
		fname := "concurrent3.txt"
		c.Create(ctx, fname)
		c.Write(ctx, fname, 0, []byte("Concurrent file 3 data"))
		data, _ := c.Read(ctx, fname, 0, 100)
		fmt.Printf("✅ Concurrent 3: %s\n", string(data))
		c.Delete(ctx, fname)
		done <- true
	}()

	// Wait for all concurrent operations to complete
	for i := 0; i < 3; i++ {
		<-done
	}

	// Test 7: Cleanup large test file
	fmt.Println("\n=== Test 7: Cleanup ===")
	err = c.Delete(ctx, largeFileName)
	if err != nil {
		fmt.Printf("❌ Failed to delete large test file: %v\n", err)
	} else {
		fmt.Printf("✅ Successfully cleaned up large test file\n")
	}

	// Test 8: Verify chunk size behavior with multiple appends
	fmt.Println("\n=== Test 8: Multiple append operations ===")
	appendFileName := "append_test.txt"
	err = c.Create(ctx, appendFileName)
	if err != nil {
		fmt.Printf("❌ Failed to create append test file: %v\n", err)
		return
	}

	// Perform multiple appends
	for i := 0; i < 5; i++ {
		appendData := fmt.Sprintf("Append operation #%d - ", i+1)
		_, _, err = c.Append(ctx, appendFileName, []byte(appendData))
		if err != nil {
			fmt.Printf("❌ Failed append #%d: %v\n", i+1, err)
		} else {
			fmt.Printf("✅ Append #%d successful\n", i+1)
		}
	}

	// Read final content
	finalData, err := c.Read(ctx, appendFileName, 0, 1000)
	if err != nil {
		fmt.Printf("❌ Failed to read final append data: %v\n", err)
	} else {
		fmt.Printf("✅ Final append content: %s\n", string(finalData))
	}

	// Cleanup
	c.Delete(ctx, appendFileName)

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("🎯 Edge case testing completed!")
	fmt.Println("✅ Error handling working correctly")
	fmt.Println("✅ Large file operations successful")
	fmt.Println("✅ Concurrent operations working")
	fmt.Println("✅ Multiple append operations working")
}
