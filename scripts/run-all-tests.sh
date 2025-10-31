#!/bin/bash
# Script to run all integration tests for all language bindings

set -e

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

PROJECT_ROOT=$(pwd)

# First, prepare the bindings
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Preparing bindings...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
./scripts/prepare-bindings.sh

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Running Rust Core Tests${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
cargo test --release --lib

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Running Go Integration Tests${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if command -v go &> /dev/null; then
    cd "${PROJECT_ROOT}/bindings/go/tests"
    if go test -v; then
        echo -e "${GREEN}✅ Go tests passed${NC}"
    else
        echo -e "${RED}❌ Go tests failed${NC}"
        exit 1
    fi
else
    echo -e "${YELLOW}⚠️  Go not found, skipping Go tests${NC}"
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Running Node.js Integration Tests${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if command -v node &> /dev/null; then
    cd "${PROJECT_ROOT}/bindings/napi"

    # Install dependencies if needed
    if [ ! -d "node_modules" ]; then
        echo "Installing Node dependencies..."
        npm install
        npm install --save-dev jest
    fi

    cd tests
    if npx jest integration.test.js --verbose; then
        echo -e "${GREEN}✅ Node tests passed${NC}"
    else
        echo -e "${RED}❌ Node tests failed${NC}"
        exit 1
    fi
else
    echo -e "${YELLOW}⚠️  Node.js not found, skipping Node tests${NC}"
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${BLUE}Running Python Integration Tests${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

if command -v python3 &> /dev/null; then
    cd "${PROJECT_ROOT}/bindings/python"

    # Install package in development mode if needed
    if ! python3 -c "import jasonisnthappy" 2>/dev/null; then
        echo "Installing Python package in development mode..."
        pip3 install -e . --quiet
    fi

    cd tests
    if python3 -m pytest test_integration.py -v 2>/dev/null || python3 -m unittest test_integration.py -v; then
        echo -e "${GREEN}✅ Python tests passed${NC}"
    else
        echo -e "${RED}❌ Python tests failed${NC}"
        exit 1
    fi
else
    echo -e "${YELLOW}⚠️  Python3 not found, skipping Python tests${NC}"
fi

echo ""
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✅ All tests passed successfully!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

cd "${PROJECT_ROOT}"
