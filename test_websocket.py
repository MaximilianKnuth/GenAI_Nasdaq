#!/usr/bin/env python3
import asyncio
import websockets
import json
import sys
import time

async def test_websocket():
    uri = "ws://127.0.0.1:9000/ws"
    print(f"Connecting to {uri}...")
    
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected to WebSocket server!")
            
            # Send a ping message
            print("Sending ping message...")
            await websocket.send(json.dumps({"type": "ping", "content": "ping"}))
            
            # Wait for response
            response = await websocket.recv()
            print(f"Received: {response}")
            
            # Send a simple query
            print("\nSending a test query...")
            await websocket.send(json.dumps({
                "type": "process_query",
                "content": "Please show me the first 3 rows of the EFR dataset"
            }))
            
            # Wait for responses and handle interactive flow
            print("Waiting for responses (press Ctrl+C to exit):")
            waiting_for_human_input = False
            try:
                while True:
                    response = await websocket.recv()
                    print(f"Received: {response}")
                    
                    # Parse the response
                    message = json.loads(response)
                    
                    # If the server is asking for human input
                    if message.get("type") == "human_input_request":
                        waiting_for_human_input = True
                        print("\n>>> Server is waiting for human input. What would you like to respond? (e.g. 'yes')")
                        user_input = input("Your response: ")
                        
                        # Send the human input back
                        await websocket.send(json.dumps({
                            "type": "human_input",
                            "content": user_input
                        }))
                        print(f"Sent human input: {user_input}")
                        waiting_for_human_input = False
                    
                    # If we receive a process_complete message, we're done
                    if message.get("type") == "process_complete":
                        print("\nQuery processing completed!")
                        break
                        
            except KeyboardInterrupt:
                print("\nTest interrupted. Exiting...")
                
    except Exception as e:
        print(f"Error: {e}")
        return False
        
    return True

if __name__ == "__main__":
    try:
        asyncio.run(test_websocket())
    except KeyboardInterrupt:
        print("\nTest interrupted. Exiting...")
        sys.exit(0) 