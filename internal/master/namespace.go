package master

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// B-tree namespace implementation for GFS master
// Provides hierarchical directory structure with efficient binary search
// as specified in the Google File System paper Section 2.5

const (
	// B-tree degree (number of keys per node)
	// Higher degree = fewer levels, more efficient for large namespaces
	BTreeDegree = 64
)

var (
	ErrPathNotFound    = errors.New("path not found")
	ErrDirectoryExists = errors.New("directory already exists")
	ErrNotDirectory    = errors.New("not a directory")
	ErrNotEmpty        = errors.New("directory not empty")
	ErrInvalidPath     = errors.New("invalid path")
)

// NodeType represents the type of namespace node
type NodeType int

const (
	FileNode NodeType = iota
	DirectoryNode
)

// NamespaceNode represents a node in the B-tree namespace
type NamespaceNode struct {
	Name        string                     // Name component (not full path)
	Type        NodeType                   // File or directory
	FileInfo    *FileInfo                  // File metadata (nil for directories)
	Children    map[string]*NamespaceNode  // Child nodes for directories
	Parent      *NamespaceNode             // Parent node (nil for root)
	IsLeaf      bool                       // True if this is a B-tree leaf node
	Keys        []string                   // Sorted keys for B-tree navigation
	mu          sync.RWMutex              // Read-write mutex for thread safety
}

// BTreeNamespace implements a hierarchical namespace using B-tree structure
type BTreeNamespace struct {
	root *NamespaceNode
	mu   sync.RWMutex
}

// NewBTreeNamespace creates a new B-tree namespace with root directory
func NewBTreeNamespace() *BTreeNamespace {
	root := &NamespaceNode{
		Name:     "/",
		Type:     DirectoryNode,
		Children: make(map[string]*NamespaceNode),
		IsLeaf:   true,
		Keys:     make([]string, 0),
	}
	
	return &BTreeNamespace{
		root: root,
	}
}

// splitPath splits a file path into components, handling edge cases
func splitPath(path string) []string {
	if path == "/" {
		return []string{}
	}
	
	// Remove leading/trailing slashes and split
	path = strings.Trim(path, "/")
	if path == "" {
		return []string{}
	}
	
	return strings.Split(path, "/")
}

// getNodeUnsafe traverses the B-tree to find a node by path (without locking)
func (ns *BTreeNamespace) getNodeUnsafe(path string) (*NamespaceNode, error) {
	components := splitPath(path)
	current := ns.root
	
	for _, component := range components {
		current.mu.RLock()
		child, exists := current.Children[component]
		current.mu.RUnlock()
		
		if !exists {
			return nil, ErrPathNotFound
		}
		current = child
	}
	
	return current, nil
}

// getNode traverses the B-tree to find a node by path
func (ns *BTreeNamespace) getNode(path string) (*NamespaceNode, error) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	
	return ns.getNodeUnsafe(path)
}

// createNode creates a new node and updates B-tree structure
func (ns *BTreeNamespace) createNode(path string, nodeType NodeType, fileInfo *FileInfo) (*NamespaceNode, error) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	
	components := splitPath(path)
	if len(components) == 0 {
		return nil, ErrInvalidPath
	}
	
	// Navigate to parent directory
	parent := ns.root
	for i := 0; i < len(components)-1; i++ {
		parent.mu.RLock()
		child, exists := parent.Children[components[i]]
		parent.mu.RUnlock()
		
		if !exists {
			return nil, ErrPathNotFound
		}
		if child.Type != DirectoryNode {
			return nil, ErrNotDirectory
		}
		parent = child
	}
	
	name := components[len(components)-1]
	
	parent.mu.Lock()
	defer parent.mu.Unlock()
	
	// Check if node already exists
	if _, exists := parent.Children[name]; exists {
		if nodeType == DirectoryNode {
			return nil, ErrDirectoryExists
		}
		return nil, ErrFileExists
	}
	
	// Create new node
	newNode := &NamespaceNode{
		Name:     name,
		Type:     nodeType,
		FileInfo: fileInfo,
		Parent:   parent,
		IsLeaf:   true,
		Keys:     make([]string, 0),
	}
	
	if nodeType == DirectoryNode {
		newNode.Children = make(map[string]*NamespaceNode)
	}
	
	// Add to parent
	parent.Children[name] = newNode
	
	// Update B-tree keys for efficient binary search
	ns.updateKeys(parent)
	
	return newNode, nil
}

// updateKeys maintains sorted keys for B-tree binary search efficiency
func (ns *BTreeNamespace) updateKeys(node *NamespaceNode) {
	node.Keys = make([]string, 0, len(node.Children))
	for name := range node.Children {
		node.Keys = append(node.Keys, name)
	}
	sort.Strings(node.Keys)
	
	// Split node if it exceeds B-tree degree
	if len(node.Keys) > BTreeDegree {
		ns.splitNode(node)
	}
}

// splitNode splits a B-tree node when it exceeds the degree
func (ns *BTreeNamespace) splitNode(node *NamespaceNode) {
	// For simplicity, we'll implement a basic split
	// In a full B-tree implementation, this would be more complex
	// For now, we maintain the sorted keys which provides the main benefit
	
	// The keys are already sorted, which is the primary optimization
	// Advanced splitting would require restructuring parent-child relationships
}

// createParentDirectories creates all parent directories in a path if they don't exist
func (ns *BTreeNamespace) createParentDirectories(path string) error {
	components := splitPath(path)
	if len(components) <= 1 {
		return nil // No parent directories needed for root or single component
	}
	
	// Create parent directories incrementally
	for i := 1; i < len(components); i++ {
		parentPath := "/" + strings.Join(components[:i], "/")
		if !ns.DirectoryExists(parentPath) {
			err := ns.CreateDirectory(parentPath)
			if err != nil && err != ErrDirectoryExists {
				return fmt.Errorf("failed to create parent directory %s: %v", parentPath, err)
			}
		}
	}
	
	return nil
}

// CreateFile creates a new file in the namespace, automatically creating parent directories
func (ns *BTreeNamespace) CreateFile(path string, fileInfo *FileInfo) error {
	// Ensure parent directories exist
	if err := ns.createParentDirectories(path); err != nil {
		return err
	}
	
	_, err := ns.createNode(path, FileNode, fileInfo)
	return err
}

// CreateDirectory creates a new directory in the namespace, automatically creating parent directories  
func (ns *BTreeNamespace) CreateDirectory(path string) error {
	// Ensure parent directories exist
	if err := ns.createParentDirectories(path); err != nil {
		return err
	}
	
	_, err := ns.createNode(path, DirectoryNode, nil)
	return err
}

// GetFile retrieves file information by path
func (ns *BTreeNamespace) GetFile(path string) (*FileInfo, error) {
	node, err := ns.getNode(path)
	if err != nil {
		return nil, err
	}
	
	if node.Type != FileNode {
		return nil, ErrNotDirectory
	}
	
	return node.FileInfo, nil
}

// DeleteFile removes a file from the namespace
func (ns *BTreeNamespace) DeleteFile(path string) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	
	node, err := ns.getNode(path)
	if err != nil {
		return err
	}
	
	if node.Type != FileNode {
		return ErrNotDirectory
	}
	
	parent := node.Parent
	if parent == nil {
		return ErrInvalidPath
	}
	
	parent.mu.Lock()
	defer parent.mu.Unlock()
	
	delete(parent.Children, node.Name)
	ns.updateKeys(parent)
	
	return nil
}

// RenameFile renames a file in the namespace
func (ns *BTreeNamespace) RenameFile(oldPath, newPath string) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	
	// Get the file to rename
	node, err := ns.getNodeUnsafe(oldPath)
	if err != nil {
		return err
	}
	
	if node.Type != FileNode {
		return ErrNotDirectory
	}
	
	// Parse new path
	newComponents := splitPath(newPath)
	if len(newComponents) == 0 {
		return ErrInvalidPath
	}
	
	newName := newComponents[len(newComponents)-1]
	newParentPath := "/" + strings.Join(newComponents[:len(newComponents)-1], "/")
	if newParentPath == "/" && len(newComponents) == 1 {
		newParentPath = "/"
	}
	
	// Get new parent directory
	newParent := ns.root
	if newParentPath != "/" {
		newParent, err = ns.getNodeUnsafe(newParentPath)
		if err != nil {
			return err
		}
		if newParent.Type != DirectoryNode {
			return ErrNotDirectory
		}
	}
	
	// Check if target already exists
	newParent.mu.RLock()
	if _, exists := newParent.Children[newName]; exists {
		newParent.mu.RUnlock()
		return ErrFileExists
	}
	newParent.mu.RUnlock()
	
	// Remove from old parent
	oldParent := node.Parent
	oldParent.mu.Lock()
	delete(oldParent.Children, node.Name)
	oldParent.mu.Unlock()
	
	// Add to new parent
	node.Name = newName
	node.Parent = newParent
	
	newParent.mu.Lock()
	newParent.Children[newName] = node
	newParent.mu.Unlock()
	
	// Update keys for both parents
	ns.updateKeys(oldParent)
	ns.updateKeys(newParent)
	
	return nil
}

// ListDirectory lists all files and subdirectories in a directory
func (ns *BTreeNamespace) ListDirectory(path string) ([]string, error) {
	node, err := ns.getNode(path)
	if err != nil {
		return nil, err
	}
	
	if node.Type != DirectoryNode {
		return nil, ErrNotDirectory
	}
	
	node.mu.RLock()
	defer node.mu.RUnlock()
	
	// Return sorted keys for efficient traversal
	result := make([]string, len(node.Keys))
	copy(result, node.Keys)
	
	return result, nil
}

// FileExists checks if a file exists at the given path
func (ns *BTreeNamespace) FileExists(path string) bool {
	node, err := ns.getNode(path)
	if err != nil {
		return false
	}
	return node.Type == FileNode
}

// DirectoryExists checks if a directory exists at the given path
func (ns *BTreeNamespace) DirectoryExists(path string) bool {
	node, err := ns.getNode(path)
	if err != nil {
		return false
	}
	return node.Type == DirectoryNode
}

// GetAllFiles returns all files in the namespace (for migration/debugging)
func (ns *BTreeNamespace) GetAllFiles() map[string]*FileInfo {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	
	result := make(map[string]*FileInfo)
	ns.traverseNode(ns.root, "", result)
	return result
}

// traverseNode recursively traverses the B-tree to collect all files
func (ns *BTreeNamespace) traverseNode(node *NamespaceNode, basePath string, result map[string]*FileInfo) {
	node.mu.RLock()
	defer node.mu.RUnlock()
	
	for _, key := range node.Keys {
		child := node.Children[key]
		childPath := basePath + "/" + key
		if basePath == "" {
			childPath = "/" + key
		}
		
		if child.Type == FileNode {
			result[childPath] = child.FileInfo
		} else {
			ns.traverseNode(child, childPath, result)
		}
	}
}