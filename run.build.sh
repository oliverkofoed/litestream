#!/bin/bash
set -e

# run.build.sh - Automated cross-platform builds for Litestream

# a) Ask the user for a version tag
echo "------------------------------------------------------------"
echo "Litestream Automated Build Script"
echo "------------------------------------------------------------"
read -p "Enter version tag (e.g., v0.3.13): " VERSION_TAG

if [ -z "$VERSION_TAG" ]; then
    echo "Error: Version tag cannot be empty."
    exit 1
fi

# b) Makes all the builds via goreleaser
echo ""
echo "Starting builds for version: $VERSION_TAG"
echo "Running: GORELEASER_CURRENT_TAG=$VERSION_TAG goreleaser build --snapshot --clean"
echo "------------------------------------------------------------"

GORELEASER_CURRENT_TAG=$VERSION_TAG goreleaser build --snapshot --clean

# c) Prints out the location of all the builds
echo ""
echo "------------------------------------------------------------"
echo "Builds completed successfully!"
echo "The following binaries are available in the 'dist' folder:"
echo "------------------------------------------------------------"

# Find executables in dist/, excluding hidden files and metadata
find dist -type f -executable -not -path "*/.*" | grep -v "metadata.json" | sort

echo "------------------------------------------------------------"
