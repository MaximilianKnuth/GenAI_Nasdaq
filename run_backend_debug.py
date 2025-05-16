#!/usr/bin/env python3
import sys
import traceback
import importlib.util
import os
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("debug_backend.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("debug_script")

logger.info("Attempting to import and run the backend server...")

try:
    # Get the absolute path to the backend/main.py file
    backend_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backend")
    backend_main_path = os.path.join(backend_dir, "main.py")
    
    logger.info(f"Backend main path: {backend_main_path}")
    
    # Use importlib to load the backend module
    spec = importlib.util.spec_from_file_location("backend_main", backend_main_path)
    backend_main = importlib.util.module_from_spec(spec)
    
    logger.info("Module spec created, executing module...")
    spec.loader.exec_module(backend_main)
    
    # Start the server using the imported module
    logger.info("Starting backend server on host 0.0.0.0, port 9000...")
    
    # Add a small delay to ensure logging is flushed
    time.sleep(1)
    
    # Run with log_level="info" to see more details
    backend_main.uvicorn.run(
        backend_main.app, 
        host="0.0.0.0", 
        port=9000,
        log_level="info"
    )
except Exception as e:
    logger.error(f"Error: {e}")
    logger.error("\nDetailed traceback:")
    logger.error(traceback.format_exc())
    logger.error("\nSystem path:")
    for path in sys.path:
        logger.error(f"  {path}")
    
    # Also print to stdout for immediate visibility
    print(f"Error: {e}")
    print("\nDetailed traceback:")
    traceback.print_exc()
    print("\nCheck debug_backend.log for more details") 