# NASDAQ GenAI Terminal

A natural language interface for querying and analyzing financial data using AI.

## Overview

NASDAQ GenAI Terminal is an interactive application that allows users to query financial datasets using natural language or Python code. The application leverages AI to interpret user queries, process data, and provide insightful results.

## Features

- **Natural Language Interface**: Query financial data using plain English
- **Python Code Execution**: Run Python code for custom data analysis
- **Interactive Experience**: Real-time feedback and results
- **WebSocket Communication**: Stable and responsive connection between frontend and backend
- **Multiple Dataset Support**: Access and join various financial datasets

## Project Structure

```
GenAI_Nasdaq/
├── 01_Data/                 # Financial datasets
│   ├── EFR.csv              # Sample financial data
│   ├── EQR.csv              # Sample financial data
│   └── SKMS.csv             # Sample financial data
├── backend/                 # Backend server code
│   ├── agent_functions.py   # AI agent functionality
│   ├── main.py              # FastAPI server implementation
│   └── ...
├── ui/                      # Frontend code
│   ├── src/                 # React application source
│   │   ├── App.jsx          # Main application component
│   │   └── ...
│   └── ...
├── logs/                    # Application logs
├── example_query.py         # Example script for programmatic queries
├── test_websocket.py        # WebSocket connection test script
├── websocket_test.html      # Browser-based WebSocket testing
├── start_fresh.sh           # Script to start servers with clean logs
├── usage_guide.md           # Detailed usage instructions
└── README.md                # This file
```

## Getting Started

### Prerequisites

- Python 3.8+
- Node.js 14+
- Required Python packages (see requirements.txt)

### Installation

1. Clone the repository:
   ```
   git clone <repository-url>
   cd GenAI_Nasdaq
   ```

2. Install backend dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Install frontend dependencies:
   ```
   cd ui
   npm install
   cd ..
   ```

### Running the Application

Use the provided script to start both backend and frontend servers:

```
./start_fresh.sh
```

This will:
1. Stop any existing server processes
2. Clear old log files
3. Start the backend server on port 9000
4. Start the frontend server on port 3001

Access the application at: http://localhost:3001

## Usage

See the [usage guide](usage_guide.md) for detailed instructions on how to use the application.

### Basic Query Flow

1. Enter your query in the input field
2. The system will classify your task
3. Confirm the task classification
4. View the results in the terminal output

### Example Queries

- "Please show me the first 3 rows of the EFR dataset"
- "Join EFR and EQR datasets based on ticker"
- "Calculate the average price for each ticker in the EFR dataset"
- "Convert the date column in EQR from EST to UTC"

## Testing

### WebSocket Connection Test

Run the WebSocket test script to verify connectivity:

```
python test_websocket.py
```

Or open the WebSocket test page in your browser:

```
file:///path/to/GenAI_Nasdaq/websocket_test.html
```

### Programmatic Queries

Use the example script to run queries programmatically:

```
python example_query.py
```

## Troubleshooting

If you encounter issues:

1. Check the logs in the `logs/` directory
2. Ensure both backend and frontend servers are running
3. Verify that the required data files exist in the `01_Data/` directory
4. Check browser console for any frontend errors

## License

[Specify your license here]

## Acknowledgements

- OpenAI for AI capabilities
- FastAPI for the backend framework
- React for the frontend framework 