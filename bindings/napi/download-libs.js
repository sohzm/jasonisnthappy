#!/usr/bin/env node
/**
 * Auto-download script for jasonisnthappy native library
 * Downloads the correct platform binary from GitHub releases during npm install
 */

const https = require('https');
const fs = require('fs');
const path = require('path');
const os = require('os');

const RELEASE_URL = 'https://github.com/sohzm/jasonisnthappy/releases/latest/download';

function getPlatformInfo() {
  const platform = os.platform();
  const arch = os.arch();

  if (platform === 'darwin') {
    if (arch === 'arm64') {
      return {
        dir: 'darwin-arm64',
        file: 'jasonisnthappy.darwin-arm64.node',
      };
    } else if (arch === 'x64') {
      return {
        dir: 'darwin-amd64',
        file: 'jasonisnthappy.darwin-x64.node',
      };
    }
    throw new Error(`Unsupported macOS architecture: ${arch}`);
  } else if (platform === 'linux') {
    if (arch === 'arm64') {
      return {
        dir: 'linux-arm64',
        file: 'jasonisnthappy.linux-arm64-gnu.node',
      };
    } else if (arch === 'x64') {
      return {
        dir: 'linux-amd64',
        file: 'jasonisnthappy.linux-x64-gnu.node',
      };
    }
    throw new Error(`Unsupported Linux architecture: ${arch}`);
  } else if (platform === 'win32') {
    if (arch === 'arm64') {
      throw new Error(
        'Windows ARM64 is not currently supported. ' +
        'Pre-built binaries are only available for Windows x64. ' +
        'Please use Windows x64 or build from source.'
      );
    } else if (arch === 'x64') {
      return {
        dir: 'windows-amd64',
        file: 'jasonisnthappy.win32-x64-msvc.node',
      };
    }
    throw new Error(`Unsupported Windows architecture: ${arch}`);
  }
  throw new Error(`Unsupported platform: ${platform}`);
}

function downloadFile(url, destPath) {
  return new Promise((resolve, reject) => {
    console.error(`Downloading from ${url}...`);

    const file = fs.createWriteStream(destPath);

    const request = (urlStr) => {
      https.get(urlStr, (response) => {
        // Handle redirects
        if (response.statusCode === 301 || response.statusCode === 302) {
          const redirectUrl = response.headers.location;
          console.error(`Following redirect to ${redirectUrl}...`);
          request(redirectUrl);
          return;
        }

        if (response.statusCode !== 200) {
          fs.unlinkSync(destPath);
          reject(new Error(`Failed to download: HTTP ${response.statusCode}`));
          return;
        }

        const totalSize = parseInt(response.headers['content-length'], 10);
        let downloadedSize = 0;

        response.on('data', (chunk) => {
          downloadedSize += chunk.length;
          if (totalSize > 0) {
            const percent = Math.min(Math.round((downloadedSize / totalSize) * 100), 100);
            process.stderr.write(`\rProgress: ${percent}%`);
          }
        });

        response.pipe(file);

        file.on('finish', () => {
          file.close();
          process.stderr.write('\n');
          resolve();
        });
      }).on('error', (err) => {
        fs.unlinkSync(destPath);
        reject(err);
      });
    };

    request(url);
  });
}

async function main() {
  try {
    const platformInfo = getPlatformInfo();

    // Install to lib directory within package
    const libDir = path.join(__dirname, 'lib', platformInfo.dir);
    fs.mkdirSync(libDir, { recursive: true });

    const destPath = path.join(libDir, 'jasonisnthappy.node');

    // Skip if already exists
    if (fs.existsSync(destPath)) {
      console.error(`Library already exists at ${destPath}`);
      return;
    }

    const url = `${RELEASE_URL}/${platformInfo.file}`;
    console.error(
      `Downloading jasonisnthappy native library for ${os.platform()}-${os.arch()}...`
    );

    await downloadFile(url, destPath);

    console.error(`Successfully downloaded to ${destPath}`);
  } catch (error) {
    console.error(`Failed to download native library: ${error.message}`);
    console.error('\nYou can manually download the library from:');
    console.error('https://github.com/sohzm/jasonisnthappy/releases/latest');
    console.error('And place it in the lib/<platform>/ directory.\n');
    // Don't fail the install, allow fallback to local build
    process.exit(0);
  }
}

main();
