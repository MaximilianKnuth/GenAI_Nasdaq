#!/bin/bash

# Function to stop processes on exit
cleanup() {
  echo "Stopping servers and cleaning up..."
  
  # Kill backend processes
  if [ -n "$BACKEND_PID" ]; then
    echo "Killing backend process $BACKEND_PID"
    kill $BACKEND_PID 2>/dev/null || kill -9 $BACKEND_PID 2>/dev/null
  fi
  
  # Kill frontend processes
  if [ -n "$FRONTEND_PID" ]; then
    echo "Killing frontend process $FRONTEND_PID"
    kill $FRONTEND_PID 2>/dev/null || kill -9 $FRONTEND_PID 2>/dev/null
  fi
  
  # Additional cleanup of any remaining processes
  echo "Cleaning up any remaining server processes..."
  pkill -f "python.*main.py" || true
  pkill -f "python.*run_backend_debug.py" || true
  pkill -f "npm run dev" || true
  
  # Clean up only frontend log files and temporary logs
  echo "Cleaning up frontend log files..."
  rm -f logs/frontend_fresh.log
  rm -f logs/*.tmp.log
  rm -f debug_backend.log
  rm -f workflow.mmd
  rm -f backend.log
  
  echo "Cleanup complete"
  exit 0
}

# Set up trap to catch Ctrl+C and other termination signals
trap cleanup INT TERM EXIT

# Kill any existing servers on the same ports
echo "Cleaning up any existing servers..."
pkill -f "python.*main.py" || true
pkill -f "python.*run_backend_debug.py" || true
pkill -f "npm run dev" || true
sleep 2

# Make sure we're in the script's directory
cd "$(dirname "$0")"

# Create logs directory if it doesn't exist
mkdir -p logs

# Clear old log files
echo "Clearing old log files..."
rm -f logs/*.log
rm -f *.log

# Start the backend server using our debug script
echo "Starting backend server on port 9000..."
python run_backend_debug.py > logs/backend_fresh.log 2>&1 &
BACKEND_PID=$!

# Wait a moment for backend to initialize
sleep 3

# Check if backend is running
if ! lsof -i :9000 > /dev/null; then
  echo "⚠️  Warning: Backend server doesn't seem to be running on port 9000."
  echo "Check logs/backend_fresh.log for errors."
  cat logs/backend_fresh.log | tail -20
else
  echo "✅ Backend server running on port 9000"
fi

# Start the frontend server
echo "Starting frontend server..."
cd ui
npm run dev -- --host --port 3001 > ../logs/frontend_fresh.log 2>&1 &
FRONTEND_PID=$!
cd ..

# Wait a moment for frontend to initialize
sleep 3

# Check if frontend is running
if ! lsof -i :3001 > /dev/null; then
  echo "⚠️  Warning: Frontend server doesn't seem to be running on port 3001."
  echo "Check logs/frontend_fresh.log for errors."
  cat logs/frontend_fresh.log | tail -20
else
  echo "✅ Frontend server running on port 3001"
fi

# Print instructions
echo ""
echo "===== ACCESS INSTRUCTIONS ====="
echo "1. Open your browser and navigate to:"
echo "   http://127.0.0.1:3001"
echo ""
echo "2. If that doesn't work, try:"
echo "   http://localhost:3001"
echo ""
echo "3. For WebSocket testing, use:"
echo "   python test_websocket.py"
echo ""
echo "4. Press Ctrl+C to stop both servers"
echo ""
echo "5. Check logs in the logs directory if you encounter issues"
echo "=============================="

# Wait for both processes
wait $BACKEND_PID $FRONTEND_PID 