// +build ignore

// Install command for downloading jasonisnthappy native libraries
// Run with: go run github.com/sohzm/jasonisnthappy/bindings/go/install.go
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
)

const releaseURL = "https://github.com/sohzm/jasonisnthappy/releases/latest/download"

type platformInfo struct {
	dir  string
	file string
	dest string
}

func getPlatformInfo() *platformInfo {
	switch runtime.GOOS {
	case "darwin":
		if runtime.GOARCH == "arm64" {
			return &platformInfo{
				dir:  "darwin-arm64",
				file: "darwin-arm64-dynamic.dylib",
				dest: "libjasonisnthappy.dylib",
			}
		}
		return &platformInfo{
			dir:  "darwin-amd64",
			file: "darwin-amd64-dynamic.dylib",
			dest: "libjasonisnthappy.dylib",
		}
	case "linux":
		if runtime.GOARCH == "arm64" {
			return &platformInfo{
				dir:  "linux-arm64",
				file: "linux-arm64-dynamic.so",
				dest: "libjasonisnthappy.so",
			}
		}
		return &platformInfo{
			dir:  "linux-amd64",
			file: "linux-amd64-dynamic.so",
			dest: "libjasonisnthappy.so",
		}
	case "windows":
		if runtime.GOARCH == "arm64" {
			fmt.Fprintf(os.Stderr, "Windows ARM64 is not currently supported.\n")
			return nil
		}
		return &platformInfo{
			dir:  "windows-amd64",
			file: "windows-amd64-dynamic.dll",
			dest: "jasonisnthappy.dll",
		}
	}
	return nil
}

func main() {
	platform := getPlatformInfo()
	if platform == nil {
		fmt.Fprintf(os.Stderr, "Unsupported platform: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		os.Exit(1)
	}

	// Install to lib directory in Go module
	// Find the module directory
	libDir := filepath.Join("lib", platform.dir)
	if err := os.MkdirAll(libDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create lib directory: %v\n", err)
		os.Exit(1)
	}

	destPath := filepath.Join(libDir, platform.dest)

	// Skip if already exists
	if _, err := os.Stat(destPath); err == nil {
		fmt.Printf("✓ Library already exists at %s\n", destPath)
		return
	}

	url := fmt.Sprintf("%s/%s", releaseURL, platform.file)
	fmt.Printf("Downloading jasonisnthappy native library for %s-%s...\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("URL: %s\n", url)

	// Download
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to download: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "Download failed: HTTP %d\n", resp.StatusCode)
		os.Exit(1)
	}

	out, err := os.Create(destPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create file: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()

	// Copy with progress
	total := resp.ContentLength
	var downloaded int64
	buf := make([]byte, 32*1024)
	for {
		n, err := resp.Body.Read(buf)
		if n > 0 {
			out.Write(buf[:n])
			downloaded += int64(n)
			if total > 0 {
				percent := float64(downloaded) / float64(total) * 100
				fmt.Printf("\rProgress: %.1f%%", percent)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			os.Remove(destPath)
			fmt.Fprintf(os.Stderr, "\nFailed to download: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Printf("\n✓ Successfully downloaded to %s\n", destPath)
}
