#!/bin/bash

# Function to stop processes on exit
cleanup() {
  echo "Stopping servers..."
  if [ -n "$BACKEND_PID" ]; then
    echo "Killing backend process $BACKEND_PID"
    kill $BACKEND_PID 2>/dev/null || kill -9 $BACKEND_PID 2>/dev/null
  fi
  
  if [ -n "$FRONTEND_PID" ]; then
    echo "Killing frontend process $FRONTEND_PID"
    kill $FRONTEND_PID 2>/dev/null || kill -9 $FRONTEND_PID 2>/dev/null
  fi
  
  echo "Cleanup complete"
  exit 0
}

# Set up trap to catch Ctrl+C and other termination signals
trap cleanup INT TERM

# Kill any existing servers on the same ports
echo "Cleaning up any existing servers..."
pkill -f "python.*main.py" || true
pkill -f "npm run dev" || true
sleep 2

# Make sure we're in the script's directory
cd "$(dirname "$0")"

# Create logs directory if it doesn't exist
mkdir -p logs

# Start the backend server
echo "Starting backend server on port 9000..."
cd backend
python main.py > ../logs/backend.log 2>&1 &
BACKEND_PID=$!
cd ..

# Wait a moment for backend to initialize
sleep 3

# Check if backend is running
if ! lsof -i :9000 > /dev/null; then
  echo "⚠️  Warning: Backend server doesn't seem to be running on port 9000."
  echo "Check logs/backend.log for errors."
  cat logs/backend.log | tail -20
else
  echo "✅ Backend server running on port 9000"
fi

# Start the frontend server
echo "Starting frontend server..."
cd ui
npm run dev -- --host --port 3000 > ../logs/frontend.log 2>&1 &
FRONTEND_PID=$!
cd ..

# Wait a moment for frontend to initialize
sleep 3

# Check if frontend is running
FRONTEND_PORT=$(lsof -i -P | grep LISTEN | grep node | awk '{print $9}' | cut -d':' -f2 | head -1)
if [ -z "$FRONTEND_PORT" ]; then
  echo "⚠️  Warning: Frontend server doesn't seem to be running."
  echo "Check logs/frontend.log for errors."
  cat logs/frontend.log | tail -20
else
  echo "✅ Frontend server running on port $FRONTEND_PORT"
fi

# Create a simple test page for direct WebSocket testing
echo "Creating WebSocket test page..."
cat > websocket_direct_test.html << 'EOL'
<!DOCTYPE html>
<html>
<head>
    <title>Direct WebSocket Test</title>
    <style>
        body { font-family: Arial, sans-serif; padding: 20px; }
        #log { border: 1px solid #ccc; padding: 10px; height: 300px; overflow-y: auto; }
        button { padding: 8px 16px; margin: 10px 0; }
    </style>
</head>
<body>
    <h1>Direct WebSocket Test</h1>
    <p>This is a minimal test page with no dependencies.</p>
    <button id="connect">Connect</button>
    <button id="disconnect" disabled>Disconnect</button>
    <button id="send" disabled>Send Test</button>
    <div id="log"></div>
    
    <script>
        const log = document.getElementById('log');
        const connectBtn = document.getElementById('connect');
        const disconnectBtn = document.getElementById('disconnect');
        const sendBtn = document.getElementById('send');
        let socket = null;
        
        function addLog(msg) {
            const line = document.createElement('div');
            line.textContent = new Date().toLocaleTimeString() + ': ' + msg;
            log.appendChild(line);
            log.scrollTop = log.scrollHeight;
        }
        
        connectBtn.addEventListener('click', () => {
            try {
                socket = new WebSocket('ws://127.0.0.1:9000/ws');
                addLog('Connecting...');
                
                socket.onopen = () => {
                    addLog('Connected!');
                    connectBtn.disabled = true;
                    disconnectBtn.disabled = false;
                    sendBtn.disabled = false;
                };
                
                socket.onmessage = (event) => {
                    addLog('Received: ' + event.data);
                };
                
                socket.onclose = () => {
                    addLog('Connection closed');
                    connectBtn.disabled = false;
                    disconnectBtn.disabled = true;
                    sendBtn.disabled = true;
                };
                
                socket.onerror = (error) => {
                    addLog('Error: ' + error);
                };
            } catch (error) {
                addLog('Error: ' + error.message);
            }
        });
        
        disconnectBtn.addEventListener('click', () => {
            if (socket) {
                socket.close();
                socket = null;
            }
        });
        
        sendBtn.addEventListener('click', () => {
            if (socket && socket.readyState === WebSocket.OPEN) {
                const msg = JSON.stringify({
                    type: 'process_query',
                    content: 'Test query'
                });
                socket.send(msg);
                addLog('Sent: ' + msg);
            }
        });
    </script>
</body>
</html>
EOL

# Print instructions
echo ""
echo "===== ACCESS INSTRUCTIONS ====="
echo "1. Open your browser and navigate to:"
echo "   http://127.0.0.1:${FRONTEND_PORT:-3000}"
echo ""
echo "2. If that doesn't work, try:"
echo "   http://localhost:${FRONTEND_PORT:-3000}"
echo ""
echo "3. For WebSocket testing, open either:"
echo "   http://127.0.0.1:9000/test (served by backend)"
echo "   or"
echo "   file://$(pwd)/websocket_direct_test.html (minimal test page)"
echo ""
echo "4. Press Ctrl+C to stop both servers"
echo ""
echo "5. Check logs in the logs directory if you encounter issues"
echo "=============================="

# Wait for both processes
wait $BACKEND_PID $FRONTEND_PID 