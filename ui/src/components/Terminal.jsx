import React, { useEffect, useRef } from 'react';
import './Terminal.css';

export const Terminal = ({ 
  messages, 
  inputValue, 
  setInputValue, 
  handleSubmit, 
  isProcessing, 
  connected,
  waitingForHumanInput,
  humanInputPrompt
}) => {
  const messagesEndRef = useRef(null);
  
  // Auto-scroll to bottom when messages change
  useEffect(() => {
    if (messagesEndRef.current) {
      messagesEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages]);
  
  // Render different message types
  const renderMessage = (message, index) => {
    switch (message.type) {
      case 'system':
        return (
          <div key={index} className="message system-message">
            <span className="prefix">System:</span> {message.content}
          </div>
        );
      case 'user_input':
        return (
          <div key={index} className="message user-message">
            <span className="prefix">You:</span> {message.content}
          </div>
        );
      case 'human_input_request':
        return (
          <div key={index} className="message prompt-message">
            <span className="prefix">Prompt:</span> {message.content}
          </div>
        );
      case 'human_response':
        return (
          <div key={index} className="message response-message">
            <span className="prefix">Response:</span> {message.content}
          </div>
        );
      case 'error':
        return (
          <div key={index} className="message error-message">
            <span className="prefix">Error:</span> {message.content}
          </div>
        );
      case 'result':
        return (
          <div key={index} className="message result-message">
            <span className="prefix">Result:</span> 
            <pre className="result-content">
              {typeof message.content === 'object' 
                ? JSON.stringify(message.content, null, 2) 
                : message.content}
            </pre>
          </div>
        );
      case 'terminal_output':
        return (
          <div key={index} className="message output-message">
            <pre>{message.content}</pre>
          </div>
        );
      case 'process_complete':
        return (
          <div key={index} className="message system-message">
            <span className="prefix">System:</span> Query processing completed. Generated code is saved to <code>generated_code.py</code> and results are available as CSV files in the root directory.
          </div>
        );
      default:
        return (
          <div key={index} className="message unknown-message">
            <pre>{JSON.stringify(message, null, 2)}</pre>
          </div>
        );
    }
  };
  
  return (
    <div className="terminal">
      <div className="terminal-messages">
        {messages.length === 0 ? (
          <div className="welcome-message">
            <h2>Welcome to NASDAQ GenAI Terminal</h2>
            <p>
              This terminal allows you to run data analysis queries against financial data.
              Type your query in natural language or use a slash (/) for direct Python code.
            </p>
            <p>
              Example: <code>"Please join EFR and EQR based on ticker"</code>
            </p>
            <p>
              Or Python: <code>/pd.DataFrame(&#123;"ticker": ["AAPL", "MSFT", "GOOGL"], "price": [150, 250, 2800]&#125;)</code>
            </p>
          </div>
        ) : (
          messages.map(renderMessage)
        )}
        <div ref={messagesEndRef} />
      </div>
      
      <form onSubmit={handleSubmit} className="terminal-input">
        <input
          type="text"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          placeholder={
            !connected ? "Connecting..." : 
            waitingForHumanInput ? "Type your response..." : 
            "Type your query or /python_code..."
          }
          disabled={!connected || (isProcessing && !waitingForHumanInput)}
          className={waitingForHumanInput ? "human-input" : ""}
        />
        <button 
          type="submit" 
          disabled={!connected || (isProcessing && !waitingForHumanInput) || !inputValue.trim()}
          className={waitingForHumanInput ? "human-input-button" : ""}
        >
          {waitingForHumanInput ? "Respond" : 
           isProcessing ? "Processing..." : "Send"}
        </button>
      </form>
      
      {waitingForHumanInput && (
        <div className="human-input-indicator">
          <div className="pulse"></div>
          <span>Waiting for your input...</span>
        </div>
      )}
    </div>
  );
}; 