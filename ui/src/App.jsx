import React, { useState, useEffect, useRef } from 'react';
import './App.css';
import { Terminal } from './components/Terminal';

function App() {
  const [connected, setConnected] = useState(false);
  const [messages, setMessages] = useState([]);
  const [inputValue, setInputValue] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [waitingForHumanInput, setWaitingForHumanInput] = useState(false);
  const [humanInputPrompt, setHumanInputPrompt] = useState('');
  const [connectionStatus, setConnectionStatus] = useState('disconnected'); // 'disconnected', 'connecting', 'connected'
  
  const ws = useRef(null);
  const reconnectingRef = useRef(false);
  const attemptedConnectionsRef = useRef(0);
  const maxReconnectAttemptsRef = useRef(5);
  const reconnectTimeoutRef = useRef(null);
  const heartbeatIntervalRef = useRef(null);
  
  // Function to start the heartbeat
  const startHeartbeat = () => {
    stopHeartbeat(); // Clear any existing interval
    heartbeatIntervalRef.current = setInterval(() => {
      if (ws.current && ws.current.readyState === WebSocket.OPEN) {
        console.log('Sending ping');
        ws.current.send(JSON.stringify({ type: 'ping' }));
      }
    }, 30000); // Send ping every 30 seconds
  };
  
  // Function to stop the heartbeat
  const stopHeartbeat = () => {
    if (heartbeatIntervalRef.current) {
      clearInterval(heartbeatIntervalRef.current);
      heartbeatIntervalRef.current = null;
    }
  };

  // Function to connect WebSocket with retry logic
  const connectWebSocket = () => {
    // If already connected or reconnecting, don't try to connect again
    if ((ws.current && ws.current.readyState === WebSocket.OPEN) || 
        (ws.current && ws.current.readyState === WebSocket.CONNECTING) || 
        reconnectingRef.current) {
      return;
    }
    
    reconnectingRef.current = true;
    setConnectionStatus('connecting');
    
    // Close any existing connection
    if (ws.current) {
      try {
        ws.current.close();
      } catch (err) {
        console.error('Error closing existing WebSocket:', err);
      }
    }
    
    // Try different WebSocket URLs in order of preference
    const urls = [
      'ws://127.0.0.1:9000/ws',
      'ws://localhost:9000/ws'
    ];
    
    const url = urls[0]; // Start with the first URL
    console.log(`Attempting to connect to WebSocket at ${url}`);
    
    try {
      ws.current = new WebSocket(url);
      
      ws.current.onopen = () => {
        console.log('WebSocket connection established');
        setConnected(true);
        setConnectionStatus('connected');
        reconnectingRef.current = false;
        attemptedConnectionsRef.current = 0; // Reset the counter on successful connection
        
        // Add a welcome message
        setMessages(prev => [...prev, { 
          type: 'system', 
          content: 'Connected to server. Ready to process queries.' 
        }]);
        
        // Start the heartbeat
        startHeartbeat();
      };
      
      ws.current.onmessage = (event) => {
        try {
          const message = JSON.parse(event.data);
          console.log('Received message:', message);
          
          if (message.type === 'pong') {
            console.log('Received pong from server');
            return; // Don't display pongs in the UI
          }
          
          if (message.type === 'process_complete') {
            setIsProcessing(false);
            setWaitingForHumanInput(false);
            setHumanInputPrompt('');
          }
          
          if (message.type === 'human_input_request') {
            setWaitingForHumanInput(true);
            setHumanInputPrompt(message.content);
            // Add the prompt to the messages
            setMessages(prev => [...prev, { 
              type: 'human_input_request', 
              content: message.content 
            }]);
            return; // Don't add the message again below
          }
          
          setMessages(prev => [...prev, message]);
        } catch (error) {
          console.error('Error parsing WebSocket message:', error);
          setMessages(prev => [...prev, { 
            type: 'error', 
            content: `Error parsing message: ${error.message}` 
          }]);
        }
      };
      
      ws.current.onclose = (event) => {
        console.log(`WebSocket closed with code ${event.code}`);
        setConnected(false);
        setConnectionStatus('disconnected');
        setIsProcessing(false);
        setWaitingForHumanInput(false);
        stopHeartbeat();
        
        // Only attempt to reconnect if:
        // 1. We were previously connected (not on initial connection failures)
        // 2. This wasn't a normal closure (code 1000)
        // 3. We haven't exceeded max reconnect attempts
        if (attemptedConnectionsRef.current < maxReconnectAttemptsRef.current && event.code !== 1000) {
          attemptedConnectionsRef.current++;
          const delay = Math.min(1000 * Math.pow(2, attemptedConnectionsRef.current), 30000);
          console.log(`Reconnecting in ${delay}ms (attempt ${attemptedConnectionsRef.current})`);
          
          reconnectTimeoutRef.current = setTimeout(() => {
            reconnectingRef.current = false;
            connectWebSocket();
          }, delay);
          
          setMessages(prev => [...prev, { 
            type: 'system', 
            content: `Connection lost. Reconnecting in ${delay/1000} seconds...` 
          }]);
        } else if (event.code === 1000) {
          // Normal closure
          setMessages(prev => [...prev, { 
            type: 'system', 
            content: 'Disconnected from server.' 
          }]);
        } else {
          // Max reconnect attempts exceeded
          setMessages(prev => [...prev, { 
            type: 'error', 
            content: 'Could not connect to server after multiple attempts. Please refresh the page to try again.' 
          }]);
        }
      };
      
      ws.current.onerror = (error) => {
        console.error('WebSocket error:', error);
        setMessages(prev => [...prev, { 
          type: 'error', 
          content: 'WebSocket connection error. See console for details.' 
        }]);
      };
    } catch (error) {
      console.error('Error creating WebSocket:', error);
      reconnectingRef.current = false;
      setConnectionStatus('disconnected');
      setMessages(prev => [...prev, { 
        type: 'error', 
        content: `Error creating WebSocket: ${error.message}` 
      }]);
    }
  };

  // Connect WebSocket on component mount
  useEffect(() => {
    connectWebSocket();
    
    // Cleanup on unmount
    return () => {
      stopHeartbeat();
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
      if (ws.current) {
        ws.current.close(1000, "Component unmounting");
      }
    };
  }, []); // Empty dependency array means this runs once on mount

  const handleSubmit = (e) => {
    e.preventDefault();
    if (!inputValue.trim() || !connected) return;
    
    try {
      // Check if we're waiting for human input or processing a query
      if (waitingForHumanInput) {
        // Send as human input
        ws.current.send(JSON.stringify({
          type: 'human_input',
          content: inputValue
        }));
        
        // Add the user input to the messages
        setMessages(prev => [...prev, {
          type: 'human_response',
          content: inputValue
        }]);
        
        // Reset the waiting state
        setWaitingForHumanInput(false);
        setHumanInputPrompt('');
      } else {
        // Check if the input is a query (starts with /) or a natural language query
        const isExplicitQuery = inputValue.trim().startsWith('/');
        const query = isExplicitQuery ? inputValue.trim().substring(1) : inputValue.trim();
        
        ws.current.send(JSON.stringify({
          type: 'process_query',
          content: query
        }));
        
        setIsProcessing(true);
        
        // Add the user input to the messages
        setMessages(prev => [...prev, {
          type: 'user_input',
          content: inputValue
        }]);
      }
      
      // Clear the input field
      setInputValue('');
    } catch (error) {
      console.error('Error sending message:', error);
      setMessages(prev => [...prev, {
        type: 'error',
        content: `Error sending message: ${error.message}`
      }]);
    }
  };

  return (
    <div className="App">
      <header className="App-header">
        <h1>NASDAQ GenAI Terminal</h1>
        <div className={`connection-status ${connectionStatus}`}>
          {connectionStatus === 'connected' ? 'Connected' : 
           connectionStatus === 'connecting' ? 'Connecting...' : 'Disconnected'}
        </div>
      </header>
      <main>
        <Terminal 
          messages={messages} 
          inputValue={inputValue} 
          setInputValue={setInputValue}
          handleSubmit={handleSubmit}
          isProcessing={isProcessing}
          connected={connected}
          waitingForHumanInput={waitingForHumanInput}
          humanInputPrompt={humanInputPrompt}
        />
      </main>
      <footer>
        {waitingForHumanInput ? (
          <p className="input-prompt">Please respond to the above prompt</p>
        ) : (
          <>
            <p>Example queries: "Please join EFR and EQR based on ticker" or "Convert the date column in SKMS table from EST to UTC"</p>
          </>
        )}
      </footer>
    </div>
  );
}

export default App; 