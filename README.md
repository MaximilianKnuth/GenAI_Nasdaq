# NASDAQ GenAI Terminal

![NASDAQ GenAI Terminal Interface](assets/terminal_screenshot.png)

> **Note**: Replace the image path above with your actual screenshot once you've added it to your repository.

## 🚀 Overview

NASDAQ GenAI Terminal is an AI-powered financial data analysis tool that allows you to query stock market data using natural language. Simply type what you want to know, and the system will understand your intent, process the relevant data, and return meaningful results.

**[View Demo Video](https://youtu.be/your-demo-video)** *(Replace with your actual demo video link)*

## ✨ Features

- **Natural Language Queries**: Ask questions about financial data in plain English
- **Multiple Data Operations**: 
  - Convert datetime between timezones
  - Join multiple datasets
  - Filter and analyze financial data
- **Real-time Processing**: Get instant feedback as your query is processed
- **Interactive Confirmation**: Review and confirm the system's understanding of your request
- **Terminal-like Interface**: Familiar experience for finance professionals

## 📊 Supported Datasets

The system comes pre-loaded with sample financial datasets:

- **EFR.csv**: Stock price and volume data
- **EQR.csv**: Extended stock data with timezone information
- **SKMS.csv**: Additional market data for analysis

## 🛠️ Installation

### Prerequisites

- Python 3.8+
- Node.js 14+
- npm or yarn

### Setup

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/GenAI_Nasdaq.git
   cd GenAI_Nasdaq
   ```

2. Install backend dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Install frontend dependencies:
   ```bash
   cd ui
   npm install
   cd ..
   ```

4. Make the start script executable:
   ```bash
   chmod +x start_fresh.sh
   ```

## 🚀 Running the Application

1. Start both servers with one command:
   ```bash
   ./start_fresh.sh
   ```

2. Access the application:
   - Main interface: [http://localhost:3001](http://localhost:3001) or [http://127.0.0.1:3001](http://127.0.0.1:3001)
   - WebSocket test page: Open `websocket_test.html` in your browser

## 💡 Usage Examples

### Example 1: Convert Datetime

```
Convert the New_date column in SKMS.csv from US/Eastern to UTC timezone
```

### Example 2: Join Datasets

```
Join EFR.csv and EQR.csv on ticker and date columns
```

### Example 3: Data Analysis

```
Calculate the average price for AAPL stock from EQR.csv
```

## 🔍 How It Works

![System Architecture](assets/architecture_diagram.png)

1. **User Input**: Enter your query in the terminal interface
2. **Task Classification**: The system identifies what you're trying to accomplish
3. **Confirmation**: Review and confirm the detected tasks
4. **Processing**: The system executes the necessary data operations
5. **Results**: View the results directly in the terminal interface

## 🛠️ Troubleshooting

If you encounter issues:

1. Check the logs in the `logs` directory
2. Ensure both servers are running (backend on port 9000, frontend on port 3001)
3. Try using `127.0.0.1` instead of `localhost` if connection issues occur
4. For Safari users, enable developer tools and disable cross-origin restrictions

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🙏 Acknowledgements

- OpenAI for providing the AI capabilities
- FastAPI for the backend framework
- React for the frontend framework 