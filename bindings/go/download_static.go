// +build ignore

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

func main() {
	platform := getPlatformInfo()
	if platform == nil {
		if runtime.GOOS == "windows" && runtime.GOARCH == "arm64" {
			fmt.Fprintf(os.Stderr, "❌ Windows ARM64 is not currently supported\n")
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "❌ Unsupported platform: %s/%s\n", runtime.GOOS, runtime.GOARCH)
		os.Exit(1)
	}

	// Determine library directory
	libDir := filepath.Join("lib", platform.dir)
	libPath := filepath.Join(libDir, platform.dest)

	// Check if library already exists
	if _, err := os.Stat(libPath); err == nil {
		fmt.Fprintf(os.Stderr, "✓ Static library already exists at %s\n", libPath)
		return
	}

	// Create lib directory
	if err := os.MkdirAll(libDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to create lib directory: %v\n", err)
		os.Exit(1)
	}

	url := fmt.Sprintf("%s/%s", releaseURL, platform.file)
	fmt.Fprintf(os.Stderr, "📥 Downloading static library for %s/%s...\n", runtime.GOOS, runtime.GOARCH)
	fmt.Fprintf(os.Stderr, "   URL: %s\n", url)

	// Download
	resp, err := http.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to download: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		fmt.Fprintf(os.Stderr, "❌ Download failed: HTTP %d\n", resp.StatusCode)
		fmt.Fprintf(os.Stderr, "   Make sure the release exists at: %s\n", url)
		os.Exit(1)
	}

	out, err := os.Create(libPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to create file: %v\n", err)
		os.Exit(1)
	}
	defer out.Close()

	if _, err := io.Copy(out, resp.Body); err != nil {
		os.Remove(libPath)
		fmt.Fprintf(os.Stderr, "❌ Failed to save file: %v\n", err)
		os.Exit(1)
	}

	fmt.Fprintf(os.Stderr, "✓ Successfully downloaded to %s\n", libPath)
	fmt.Fprintf(os.Stderr, "✓ You can now run 'go build' to create a static binary\n")
}

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
				file: "darwin-arm64-static.a",
				dest: "libjasonisnthappy.a",
			}
		}
		return &platformInfo{
			dir:  "darwin-amd64",
			file: "darwin-amd64-static.a",
			dest: "libjasonisnthappy.a",
		}
	case "linux":
		if runtime.GOARCH == "arm64" {
			return &platformInfo{
				dir:  "linux-arm64",
				file: "linux-arm64-static.a",
				dest: "libjasonisnthappy.a",
			}
		}
		return &platformInfo{
			dir:  "linux-amd64",
			file: "linux-amd64-static.a",
			dest: "libjasonisnthappy.a",
		}
	case "windows":
		if runtime.GOARCH == "arm64" {
			return nil
		}
		return &platformInfo{
			dir:  "windows-amd64",
			file: "windows-amd64-static.lib",
			dest: "jasonisnthappy.lib",
		}
	}
	return nil
}
