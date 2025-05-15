#!/usr/bin/env python3
import asyncio
import websockets
import json
import sys

# Example queries to demonstrate different capabilities
EXAMPLE_QUERIES = [
    "Please join EFR and EQR based on ticker",
    "Please convert the date column in the EQR dataset from EST timezone to UTC timezone"
]

async def run_query(query):
    """Run a query against the NASDAQ GenAI Terminal"""
    uri = "ws://127.0.0.1:9000/ws"
    print(f"\n{'='*80}")
    print(f"Running query: '{query}'")
    print(f"{'='*80}")
    
    try:
        async with websockets.connect(uri) as websocket:
            print("Connected to WebSocket server!")
            
            # Send the query
            await websocket.send(json.dumps({
                "type": "process_query",
                "content": query
            }))
            print(f"Sent query: {query}")
            
            # Handle the interactive flow
            waiting_for_human_input = False
            try:
                while True:
                    response = await websocket.recv()
                    message = json.loads(response)
                    
                    # Print the message type and content
                    msg_type = message.get("type", "unknown")
                    content = message.get("content", "")
                    
                    if msg_type == "system":
                        print(f"\n[System] {content}")
                    elif msg_type == "terminal_output":
                        print(f"[Output] {content}")
                    elif msg_type == "error":
                        print(f"\n[Error] {content}")
                    elif msg_type == "human_input_request":
                        print(f"\n[Question] {content}")
                        user_input = input("Your response: ")
                        
                        # Send the human input back
                        await websocket.send(json.dumps({
                            "type": "human_input",
                            "content": user_input
                        }))
                    elif msg_type == "process_complete":
                        print(f"\n[Complete] {content}")
                        break
                    else:
                        print(f"[{msg_type}] {content}")
                        
            except KeyboardInterrupt:
                print("\nQuery interrupted by user.")
                return False
                
    except Exception as e:
        print(f"Error: {e}")
        return False
        
    print(f"\nQuery completed: '{query}'")
    return True

async def main():
    """Main function to run example queries"""
    print("NASDAQ GenAI Terminal - Example Queries")
    print("---------------------------------------")
    
    # List available queries
    print("Available example queries:")
    for i, query in enumerate(EXAMPLE_QUERIES, 1):
        print(f"{i}. {query}")
    
    while True:
        try:
            choice = input("\nEnter query number to run (or 'q' to quit): ")
            
            if choice.lower() == 'q':
                break
                
            try:
                index = int(choice) - 1
                if 0 <= index < len(EXAMPLE_QUERIES):
                    await run_query(EXAMPLE_QUERIES[index])
                else:
                    print("Invalid query number. Please try again.")
            except ValueError:
                # If the input is not a number, treat it as a custom query
                await run_query(choice)
                
        except KeyboardInterrupt:
            print("\nProgram interrupted by user.")
            break
    
    print("\nThank you for using NASDAQ GenAI Terminal!")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nProgram terminated by user.")
        sys.exit(0) 