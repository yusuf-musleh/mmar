#!/bin/sh

set -e

REPO="yusuf-musleh/mmar"
BINARY="mmar"

echo "Installing $BINARY..."

# Detect OS
OS="$(uname -s)"
case "$OS" in
    Linux)  OS_TITLE="Linux";;
    Darwin) OS_TITLE="Darwin";;
    *)      echo "Unsupported OS: $OS"; exit 1;;
esac

# Detect ARCH
ARCH="$(uname -m)"
case "$ARCH" in
    x86_64) ARCH_ID="x86_64";;
    i386)   ARCH_ID="i386";;
    aarch64|arm64) ARCH_ID="arm64";;
    *) echo "Unsupported architecture: $ARCH"; exit 1;;
esac

ASSET="${BINARY}_${OS_TITLE}_${ARCH_ID}.tar.gz"
URL="https://github.com/$REPO/releases/latest/download/$ASSET"

# Temp dir
TMP_DIR=$(mktemp -d)
cd "$TMP_DIR"

echo "Downloading $ASSET..."
curl -sSL "$URL" -o "$ASSET"

echo "Extracting..."
tar -xzf "$ASSET"

# Install location
INSTALL_DIR="/usr/local/bin"

# Ensure /usr/local/lib exists
if [ ! -d "$INSTALL_DIR" ]; then
    sudo mkdir -p "$INSTALL_DIR"
fi

echo "Installing to $INSTALL_DIR"
sudo install -m 755 "$BINARY" "$INSTALL_DIR/$BINARY"

echo "$BINARY installed successfully to $INSTALL_DIR/$BINARY"
"$BINARY" version || true
