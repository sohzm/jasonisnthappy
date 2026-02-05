// Install command for jasonisnthappy Go bindings
// Downloads the native static library to the module cache
//
// Usage:
//   go run github.com/sohzm/jasonisnthappy/bindings/go/cmd/install@latest
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	repo    = "sohzm/jasonisnthappy"
	baseURL = "https://github.com/" + repo + "/releases/latest/download"
	module  = "github.com/sohzm/jasonisnthappy/bindings/go"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Determine platform
	platform := getPlatform()
	if platform == "" {
		return fmt.Errorf("unsupported platform: %s/%s", runtime.GOOS, runtime.GOARCH)
	}

	// Find module cache path
	modPath, err := findModulePath()
	if err != nil {
		return fmt.Errorf("finding module path: %w", err)
	}

	fmt.Printf("📦 Found module at: %s\n", modPath)

	// Make module writable
	if err := makeWritable(modPath); err != nil {
		return fmt.Errorf("making module writable: %w", err)
	}

	// Create lib directory
	libDir := filepath.Join(modPath, "lib", platform)
	if err := os.MkdirAll(libDir, 0755); err != nil {
		return fmt.Errorf("creating lib directory: %w", err)
	}

	// Download static library
	libPath := filepath.Join(libDir, "libjasonisnthappy.a")
	url := fmt.Sprintf("%s/%s-static.a", baseURL, platform)

	fmt.Printf("📥 Downloading static library for %s...\n", platform)
	fmt.Printf("   URL: %s\n", url)

	if err := downloadFile(url, libPath); err != nil {
		return fmt.Errorf("downloading library: %w", err)
	}

	fmt.Printf("✓ Successfully installed to %s\n", libPath)
	fmt.Println("✓ You can now run 'go build' in your project")

	return nil
}

func getPlatform() string {
	switch runtime.GOOS {
	case "darwin":
		switch runtime.GOARCH {
		case "arm64":
			return "darwin-arm64"
		case "amd64":
			return "darwin-amd64"
		}
	case "linux":
		switch runtime.GOARCH {
		case "arm64":
			return "linux-arm64"
		case "amd64":
			return "linux-amd64"
		}
	case "windows":
		if runtime.GOARCH == "amd64" {
			return "windows-amd64"
		}
	}
	return ""
}

func findModulePath() (string, error) {
	// Get GOMODCACHE
	cmd := exec.Command("go", "env", "GOMODCACHE")
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("getting GOMODCACHE: %w", err)
	}
	modCache := strings.TrimSpace(string(out))

	// Find the module directory (could be any version)
	modBase := filepath.Join(modCache, "github.com/sohzm/jasonisnthappy/bindings")
	entries, err := os.ReadDir(modBase)
	if err != nil {
		return "", fmt.Errorf("reading module cache: %w (did you run 'go get %s'?)", err, module)
	}

	// Find the latest go@ directory
	var latestPath string
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "go@") {
			latestPath = filepath.Join(modBase, entry.Name())
		}
	}

	if latestPath == "" {
		return "", fmt.Errorf("module not found in cache (did you run 'go get %s'?)", module)
	}

	return latestPath, nil
}

func makeWritable(path string) error {
	return filepath.Walk(path, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Add write permission for user
		mode := info.Mode()
		if mode.IsDir() {
			return os.Chmod(p, mode|0700)
		}
		return os.Chmod(p, mode|0600)
	})
}

func downloadFile(url, dest string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	out, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}
