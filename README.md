# GenAI Nasdaq Project

A powerful data processing and transformation system that leverages AI agents to handle complex data operations.

## Project Structure

```
GenAI_Nasdaq/
├── Agents/                    # AI agent implementations
├── Data/                     # Data storage directory
│   ├── text_data/           # Documentation and text data
│   ├── EFR.csv              # EFR dataset
│   ├── EQR.csv              # EQR dataset
│   └── SKMS.csv             # SKMS dataset
├── logs/                     # Log files directory
├── ui/                       # Frontend user interface
├── main.py                   # Main application entry point
├── agent_functions.py        # Core agent functionality
├── langgraph_implementation.py # LangGraph workflow implementation
├── requirements.txt          # Python dependencies
├── run_backend_debug.py      # Backend debugging script
├── start_fresh.sh           # Startup script with cleanup
└── state_schema.py          # State management schema
```

## Key Components

### Data (synthetic data)
- **text_data/**: Contains documentation and reference materials
- **EFR.csv**: EFR dataset
- **EQR.csv**: EQR dataset
- **SKMS.csv**: SKMS dataset

### Core Files
- **main.py**: Application entry point and main workflow
- **agent_functions.py**: Core functionality for all agents (LangGraph)
- **langgraph_implementation.py**: Implements the LangGraph workflow (LangGraph)
- **state_schema.py**: Defines the state management structure (LangGraph)
- **run_backend_debug.py**: Debugging utilities for the backend
- **start_fresh.sh**: Startup script with automatic cleanup

## Features

1. **Intelligent Data Processing**
   - Automated data validation
   - Timezone conversion
   - Table joining operations
   - Data transformation

2. **AI-Powered Operations**
   - RAG-based information retrieval
   - LLM-driven code generation
   - Smart error handling and recovery

## Getting Started

1. Clone the repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Run the application:
   ```bash
   ./start_fresh.sh
   ```

## Usage

The system supports various data operations:

1. **Timezone Conversion**
   - Convert datetime columns between different timezones
   - Automatic timezone detection and validation

2. **Table Operations**
   - Join tables based on specified columns
   - Data validation and cleaning
   - Chunking and processing large tables

## Development

- Use `start_fresh.sh` for development to ensure a clean environment
- Check `logs/` directory for operation logs
- Use `run_backend_debug.py` for debugging backend operations

## Notes

- The system automatically cleans up temporary files and logs on exit
- All data operations are logged for debugging and auditing
- The system uses both OpenAI and DeepSeek models for different operations
