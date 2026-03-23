package configpath

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Resolve returns an absolute config path using this precedence:
// 1) explicit argument (e.g. CLI flag)
// 2) environment variable value
// 3) first existing path from candidates
func Resolve(explicit, envVar string, candidates []string) (string, error) {
	if explicit != "" {
		return mustExist(explicit)
	}

	if envVar != "" {
		if fromEnv := strings.TrimSpace(os.Getenv(envVar)); fromEnv != "" {
			return mustExist(fromEnv)
		}
	}

	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		resolved, ok := resolveIfExists(candidate)
		if ok {
			return resolved, nil
		}
	}

	return "", fmt.Errorf("configuration file not found (checked candidates: %v)", candidates)
}

func mustExist(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", fmt.Errorf("failed to resolve path %q: %w", path, err)
	}

	info, err := os.Stat(abs)
	if err != nil {
		return "", fmt.Errorf("configuration file not found: %s", abs)
	}
	if info.IsDir() {
		return "", fmt.Errorf("configuration path is a directory, expected file: %s", abs)
	}

	return abs, nil
}

func resolveIfExists(path string) (string, bool) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", false
	}

	info, err := os.Stat(abs)
	if err != nil || info.IsDir() {
		return "", false
	}

	return abs, true
}
