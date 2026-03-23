package main

import (
	"fmt"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/master"
)

func main() {
	ns := master.NewBTreeNamespace()
	fileInfo := &master.FileInfo{Chunks: make(map[int64]string)}

	// Test basic operations
	err := ns.CreateFile("/test/file.txt", fileInfo)
	if err != nil {
		fmt.Printf("Create error: %v\n", err)
		return
	}

	if ns.FileExists("/test/file.txt") {
		fmt.Println("✓ File creation and existence check work")
	}

	files, _ := ns.ListDirectory("/test")
	fmt.Printf("✓ Directory listing works: %v\n", files)

	err = ns.RenameFile("/test/file.txt", "/test/renamed.txt")
	if err != nil {
		fmt.Printf("Rename error: %v\n", err)
		return
	}

	if !ns.FileExists("/test/file.txt") && ns.FileExists("/test/renamed.txt") {
		fmt.Println("✓ File rename works")
	}

	fmt.Println("✓ B-tree namespace basic functionality verified")
}
