/**
 * jasonisnthappy - Embedded document database for Node.js
 */

const path = require('path');
const os = require('os');
const fs = require('fs');

function getPlatformDir() {
  const platform = os.platform();
  const arch = os.arch();

  if (platform === 'darwin') {
    return arch === 'arm64' ? 'darwin-arm64' : 'darwin-amd64';
  } else if (platform === 'linux') {
    return arch === 'arm64' ? 'linux-arm64' : 'linux-amd64';
  } else if (platform === 'win32') {
    return 'windows-amd64';
  }
  throw new Error(`Unsupported platform: ${platform}-${arch}`);
}

function loadNativeModule() {
  const platformDir = getPlatformDir();
  const libPath = path.join(__dirname, 'lib', platformDir, 'jasonisnthappy.node');

  if (!fs.existsSync(libPath)) {
    throw new Error(
      `Native library not found at ${libPath}. ` +
      'Please run "npm install" to download the library, or check the installation.'
    );
  }

  return require(libPath);
}

module.exports = loadNativeModule();
