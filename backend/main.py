import asyncio
import os
import sys
import json
import traceback
import subprocess
import logging
import time
import tempfile
from typing import List, Dict, Any, Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
import threading
import queue
import builtins
import importlib.util

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("backend.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allow all origins for WebSocket testing
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Import DataProcessingApp from the parent directory's main.py
# using importlib to avoid circular imports
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
parent_main_path = os.path.join(parent_dir, "main.py")

# Add the parent directory to sys.path
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Import DataProcessingApp using importlib
try:
    # Use importlib to load the main module from the parent directory
    spec = importlib.util.spec_from_file_location("parent_main", parent_main_path)
    parent_main = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(parent_main)
    
    # Get the DataProcessingApp class from the loaded module
    DataProcessingApp = parent_main.DataProcessingApp
    logger.info("Successfully imported DataProcessingApp from parent main.py")
except Exception as e:
    logger.error(f"Failed to import DataProcessingApp: {e}")
    logger.error(traceback.format_exc())
    raise

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: Dict[str, WebSocket] = {}
        self.processing: Dict[str, bool] = {}
        self.app_instances: Dict[str, DataProcessingApp] = {}
        self.human_input_queues: Dict[str, queue.Queue] = {}
        self.processing_threads: Dict[str, threading.Thread] = {}
    
    async def connect(self, websocket: WebSocket):
        try:
            await websocket.accept()
            client_id = f"{websocket.client.host}:{websocket.client.port}"
            self.active_connections[client_id] = websocket
            self.processing[client_id] = False
            self.human_input_queues[client_id] = queue.Queue()
            
            # Create a new DataProcessingApp instance for this client
            try:
                self.app_instances[client_id] = DataProcessingApp()
                logger.info(f"Created DataProcessingApp instance for client {client_id}")
            except Exception as e:
                logger.error(f"Error creating DataProcessingApp for client {client_id}: {str(e)}")
                logger.error(traceback.format_exc())
                # Send error message to client
                await websocket.send_text(json.dumps({
                    "type": "error",
                    "content": f"Error initializing application: {str(e)}"
                }))
                # Close the connection
                await websocket.close()
                return
            
            logger.info(f"Client connected: {client_id}. Total connections: {len(self.active_connections)}")
            # Send welcome message
            await self.send_message(client_id, {"type": "system", "content": "Connected to NASDAQ GenAI Terminal. Type a query to begin."})
        except Exception as e:
            logger.error(f"Error during WebSocket connection: {str(e)}")
            logger.error(traceback.format_exc())
            # Don't add to active connections if accept failed
    
    def disconnect(self, websocket: WebSocket):
        try:
            client_id = f"{websocket.client.host}:{websocket.client.port}"
            if client_id in self.active_connections:
                del self.active_connections[client_id]
                self.processing.pop(client_id, None)
                
                # Clean up any running threads
                if client_id in self.processing_threads and self.processing_threads[client_id].is_alive():
                    # Can't really kill threads in Python, but we can signal them
                    if client_id in self.human_input_queues:
                        self.human_input_queues[client_id].put("__DISCONNECT__")
                
                self.human_input_queues.pop(client_id, None)
                self.app_instances.pop(client_id, None)
                self.processing_threads.pop(client_id, None)
                
                logger.info(f"Client disconnected: {client_id}. Remaining connections: {len(self.active_connections)}")
        except Exception as e:
            logger.error(f"Error during WebSocket disconnection: {str(e)}")
    
    async def send_message(self, client_id: str, message: Dict[str, Any]):
        try:
            websocket = self.active_connections.get(client_id)
            if websocket:
                await websocket.send_text(json.dumps(message))
            else:
                logger.warning(f"Attempted to send message to non-existent client: {client_id}")
        except Exception as e:
            logger.error(f"Error sending message to {client_id}: {str(e)}")
            # Connection might be broken, but we don't remove it here
    
    async def handle_ping(self, client_id: str):
        """Handle ping messages from clients"""
        try:
            await self.send_message(client_id, {"type": "pong", "content": "pong"})
            logger.debug(f"Sent pong response to {client_id}")
        except Exception as e:
            logger.error(f"Error handling ping from {client_id}: {str(e)}")
    
    def process_query_thread(self, client_id: str, query: str):
        """Thread function to process a query using DataProcessingApp"""
        try:
            logger.info(f"Starting query processing for {client_id}: {query}")
            
            # Check if the app instance exists for this client
            if client_id not in self.app_instances:
                logger.error(f"No DataProcessingApp instance found for client {client_id}")
                asyncio.run(self.send_message(client_id, {
                    "type": "error", 
                    "content": "Session error: Application instance not found. Please refresh the page and try again."
                }))
                return
            
            # Create a custom input function that gets input from our queue
            def custom_input(prompt):
                # Send the prompt to the client
                asyncio.run(self.send_message(client_id, {"type": "human_input_request", "content": prompt}))
                
                # Wait for the response
                response = self.human_input_queues[client_id].get()
                
                # Check if we got a disconnect signal
                if response == "__DISCONNECT__":
                    raise InterruptedError("Client disconnected during input")
                
                return response
            
            # Patch the built-in input function for this thread only
            original_input = builtins.input
            builtins.input = custom_input
            
            # Create a custom print function that sends output to the client
            original_print = builtins.print
            def custom_print(*args, **kwargs):
                # Call the original print
                original_print(*args, **kwargs)
                
                # Convert args to string
                output = " ".join(str(arg) for arg in args)
                
                # Send to client
                asyncio.run(self.send_message(client_id, {"type": "terminal_output", "content": output}))
            
            # Patch the built-in print function for this thread only
            builtins.print = custom_print
            
            try:
                # Process the query
                self.app_instances[client_id].process_query(query)
                
                # Send completion message
                asyncio.run(self.send_message(client_id, {"type": "process_complete", "content": "Query processing completed"}))
            finally:
                # Restore the original input and print functions
                builtins.input = original_input
                builtins.print = original_print
                
        except Exception as e:
            logger.error(f"Error processing query for {client_id}: {str(e)}")
            logger.error(traceback.format_exc())
            asyncio.run(self.send_message(client_id, {"type": "error", "content": f"Error processing query: {str(e)}"}))
        finally:
            self.processing[client_id] = False
    
    async def process_query(self, client_id: str, query: str):
        """Process a query from the client using DataProcessingApp"""
        # If already processing, inform the client
        if self.processing.get(client_id, False):
            await self.send_message(client_id, {
                "type": "system",
                "content": "Another query is already being processed. Please wait."
            })
            return
        
        try:
            self.processing[client_id] = True
            await self.send_message(client_id, {"type": "system", "content": "Processing query..."})
            
            # Start processing in a separate thread
            thread = threading.Thread(
                target=self.process_query_thread,
                args=(client_id, query),
                daemon=True
            )
            self.processing_threads[client_id] = thread
            thread.start()
            
        except Exception as e:
            logger.error(f"Error starting query processing for {client_id}: {str(e)}")
            await self.send_message(client_id, {"type": "error", "content": str(e)})
            self.processing[client_id] = False

manager = ConnectionManager()

@app.get("/")
def read_root():
    return {"message": "NASDAQ GenAI Backend API"}

@app.get("/test", response_class=HTMLResponse)
async def get_test_page():
    """Serve the WebSocket test page"""
    try:
        # First look for the test page in the current directory
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, ".."))
        test_page_path = os.path.join(project_root, "websocket_test.html")
        
        if os.path.exists(test_page_path):
            with open(test_page_path, "r") as file:
                return HTMLResponse(content=file.read())
        else:
            # If not found, create a simple test page
            return HTMLResponse(content="""
            <!DOCTYPE html>
            <html>
            <head>
                <title>WebSocket Test</title>
                <style>
                    body { font-family: Arial, sans-serif; padding: 20px; }
                    #log { border: 1px solid #ccc; padding: 10px; height: 300px; overflow-y: auto; }
                    button { padding: 8px 16px; margin: 10px 0; }
                </style>
            </head>
            <body>
                <h1>WebSocket Test</h1>
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
                            const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                            const host = window.location.host;
                            socket = new WebSocket(`${protocol}//${host}/ws`);
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
                                content: 'Please Join EFR and EQR based on ticker'
                            });
                            socket.send(msg);
                            addLog('Sent: ' + msg);
                        }
                    });
                </script>
            </body>
            </html>
            """)
    except Exception as e:
        logger.error(f"Error serving test page: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    client_id = f"{websocket.client.host}:{websocket.client.port}"
    
    try:
        while True:
            data = await websocket.receive_text()
            logger.debug(f"Received message from {client_id}: {data}")
            
            try:
                message = json.loads(data)
                message_type = message.get("type", "")
                content = message.get("content", "")
                
                if message_type == "ping":
                    await manager.handle_ping(client_id)
                elif message_type == "process_query":
                    await manager.process_query(client_id, content)
                elif message_type == "human_input":
                    # Put the human input in the queue for the processing thread
                    if client_id in manager.human_input_queues:
                        manager.human_input_queues[client_id].put(content)
                        logger.info(f"Received human input from {client_id}: {content}")
                    else:
                        await manager.send_message(client_id, {
                            "type": "error", 
                            "content": "No active query waiting for input"
                        })
                else:
                    await manager.send_message(client_id, {
                        "type": "system",
                        "content": f"Unknown message type: {message_type}"
                    })
            except json.JSONDecodeError:
                logger.warning(f"Received invalid JSON from {client_id}: {data}")
                await manager.send_message(client_id, {
                    "type": "error",
                    "content": "Invalid JSON message"
                })
            except Exception as e:
                logger.error(f"Error processing message from {client_id}: {str(e)}")
                await manager.send_message(client_id, {
                    "type": "error",
                    "content": f"Error processing message: {str(e)}"
                })
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error with {client_id}: {str(e)}")
        manager.disconnect(websocket)

# Serve static files from the parent directory
app.mount("/static", StaticFiles(directory=parent_dir), name="static")

if __name__ == "__main__":
    logger.info("Starting server on port 9000")
    uvicorn.run(app, host="0.0.0.0", port=9000) 