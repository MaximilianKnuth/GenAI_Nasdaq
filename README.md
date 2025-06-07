# NASDAQ GenAI Terminal

A conversational data pipeline assistant that helps transform data using natural language instructions.

## System Structure

### Core Components
- `backend/` - FastAPI backend server
  - `main.py` - Main FastAPI application (if without UI, will directly run from terminal)
  - `run_backend_debug.py` - Debug runner for backend
- `ui/` - React frontend application
- `logs/` - Application logs directory

### Key Files 
- `start_fresh.sh` - Main startup script (starts both backend and frontend)
- `agent_functions.py` - Core agent functionality and data processing (LangGraph)
- `state_schema.py` - State management and data structures (LangGraph)
- `requirements.txt` - Python dependencies
- `package.json` - Frontend dependencies (in ui/ directory)

### Data Files
- `SKMS.csv` - Market activity logs (main dataset)
- `EFR.csv` - Stock price and volume data
- `EQR.csv` - Extended stock data

## Installation

1. Install Python dependencies:
```bash
pip install -r requirements.txt
```

2. Install frontend dependencies:
```bash
cd ui
npm install
cd ..
```

3. Make startup script executable:
```bash
chmod +x start_fresh.sh
```

## Running the Application

1. Start both backend and frontend:
```bash
./start_fresh.sh
```

2. Access the application:
- Frontend: http://localhost:3001
- Backend API: http://localhost:9000

## System Flow

1. User inputs natural language request in web interface
2. Frontend sends request to backend via WebSocket
3. Backend processes request through agent system:
   - Task classification
   - Data validation
   - Code generation
   - Execution
4. Results are streamed back to frontend in real-time

## Troubleshooting

- Check `logs/` directory for detailed logs
- Backend runs on port 9000
- Frontend runs on port 3001
- Use `127.0.0.1` if `localhost` fails to connect

## Example Queries

1. Timezone conversion:
```
Convert the New_date column in SKMS.csv from US/Eastern to UTC
```

2. Table join:
```
Join EFR.csv and EQR.csv on ticker and date columns
```

3. Combined operations:
```
Convert New_date in SKMS.csv from EST to UTC, then join with EFR on ticker
```
