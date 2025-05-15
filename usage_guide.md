# NASDAQ GenAI Terminal - Usage Guide

## Overview

The NASDAQ GenAI Terminal is an interactive application that allows you to analyze financial data using natural language queries. The application uses WebSocket connections to communicate between the frontend and backend servers.

## Getting Started

1. Start the servers using the provided script:
   ```bash
   ./start_fresh.sh
   ```

2. Access the web interface:
   - Open your browser and navigate to: http://127.0.0.1:3001
   - If that doesn't work, try: http://localhost:3001

## Using the Application

### Basic Query Flow

1. **Enter your query in the input field**
   - Example queries:
     - "Please show me the first 3 rows of the EFR dataset"
     - "Please join EFR and EQR based on ticker"
     - "Please convert the date column in the EQR dataset from EST timezone to UTC timezone"

2. **Task Classification**
   - The system will analyze your query and identify tasks to perform
   - You will be asked to confirm if the detected tasks are correct
   - Type "yes" to proceed or provide a more precise query

3. **Interactive Process**
   - The system may ask for additional information during processing
   - For example, it might ask you to specify which column to use for joining tables
   - Simply respond to these prompts in the input field

4. **View Results**
   - Results will be displayed in the terminal interface
   - For data transformations, files will be saved in the appropriate location

### Example Query Flow

For the query "Please join EFR and EQR based on ticker":

1. Enter the query and submit
2. The system detects the task as "join_tables"
3. You confirm by typing "yes"
4. The system processes the query and displays the results

## Testing WebSocket Connection

You can test the WebSocket connection directly using the provided test script:

```bash
python test_websocket.py
```

This script will:
1. Connect to the WebSocket server
2. Send a test query
3. Handle any interactive prompts
4. Display the responses from the server

## Troubleshooting

If you encounter issues:

1. Check the logs in the `logs` directory
2. Ensure both backend and frontend servers are running
3. Try using the WebSocket test page: http://127.0.0.1:9000/test
4. Use the `test_websocket.py` script to diagnose connection issues

## Available Datasets

The application comes with several datasets:
- EFR.csv: Stock data including ticker, date, price, and volume
- EQR.csv: Stock data with additional metrics
- SKMS.csv: Market cap and other financial metrics

You can reference these datasets in your queries to perform operations on them. 