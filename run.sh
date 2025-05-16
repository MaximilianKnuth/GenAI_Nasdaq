#!/bin/bash

# Colors for better readability
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}🚀 Starting NASDAQ GenAI Terminal...${NC}"

# Ensure the logs directory exists
mkdir -p src/logs

# Cleanup existing processes
echo "Cleaning up any existing servers..."
pkill -f "uvicorn|vite|python3|python src/scripts" 2>/dev/null || true

# Clear log files
echo "Clearing old log files..."
rm -f src/logs/backend_fresh.log src/logs/frontend_fresh.log src/logs/app.log

# Set up Python environment path
export PYTHONPATH="$PYTHONPATH:$PWD"

# Process a sample query to see results
echo "Starting backend server on port 9000..."
cd ui && python3 -m uvicorn server:app --host 0.0.0.0 --port 9000 --log-level debug > ../src/logs/backend_fresh.log 2>&1 &
BACKEND_PID=$!

# Check if backend started successfully
sleep 3
if kill -0 $BACKEND_PID 2>/dev/null; then
    echo -e "${GREEN}✅ Backend server running on port 9000${NC}"
else
    echo -e "${RED}⚠️  Warning: Backend server failed to start${NC}"
    echo "Check src/logs/backend_fresh.log for errors"
    cat src/logs/backend_fresh.log
    exit 1
fi

# Start the frontend server
echo "Starting frontend server..."
cd ui && npm run dev -- --host --port 3001 > ../src/logs/frontend_fresh.log 2>&1 &
FRONTEND_PID=$!

# Check if frontend started successfully
sleep 5
if kill -0 $FRONTEND_PID 2>/dev/null; then
    echo -e "${GREEN}✅ Frontend server running on port 3001${NC}"
else
    echo -e "${RED}⚠️  Warning: Frontend server failed to start${NC}"
    echo "Check src/logs/frontend_fresh.log for errors"
    cat ../src/logs/frontend_fresh.log
    exit 1
fi

# Test backend health
echo "Testing backend health..."
curl -s http://localhost:9000/health || echo -e "${RED}⚠️ Backend health check failed${NC}"

echo -e "${YELLOW}===== ACCESS INSTRUCTIONS =====${NC}"
echo "1. Open your browser and navigate to:"
echo "   http://127.0.0.1:3001 or http://localhost:3001"
echo "2. Try using queries like:"
echo "   - Convert the date column in SKMS table from EST to UTC"
echo "   - Join EFR and EQR based on ticker"
echo "3. Press Ctrl+C to stop both servers"
echo "4. Check logs in the src/logs directory if you encounter issues"
echo -e "${YELLOW}===============================${NC}"

# Wait for Ctrl+C
trap "pkill -P $$; exit" INT
wait 