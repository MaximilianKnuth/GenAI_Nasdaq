#!/usr/bin/env python3
"""
NASDAQ GenAI Terminal - Setup Check Script
This script verifies that all necessary components are installed and configured correctly.
"""

import os
import sys
import subprocess
import importlib
import platform
import json
from pathlib import Path

# ANSI color codes for terminal output
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
BLUE = "\033[94m"
RESET = "\033[0m"
BOLD = "\033[1m"

def print_header(text):
    """Print a formatted header"""
    print(f"\n{BLUE}{BOLD}{'=' * 80}{RESET}")
    print(f"{BLUE}{BOLD} {text}{RESET}")
    print(f"{BLUE}{BOLD}{'=' * 80}{RESET}")

def print_success(text):
    """Print a success message"""
    print(f"{GREEN}✓ {text}{RESET}")

def print_warning(text):
    """Print a warning message"""
    print(f"{YELLOW}⚠ {text}{RESET}")

def print_error(text):
    """Print an error message"""
    print(f"{RED}✗ {text}{RESET}")

def print_info(text):
    """Print an info message"""
    print(f"{BLUE}ℹ {text}{RESET}")

def check_python_version():
    """Check if Python version is compatible"""
    print_header("Checking Python Version")
    
    version = sys.version_info
    version_str = f"{version.major}.{version.minor}.{version.micro}"
    print_info(f"Detected Python version: {version_str}")
    
    if version.major < 3 or (version.major == 3 and version.minor < 8):
        print_error(f"Python 3.8+ is required, but you have {version_str}")
        return False
    else:
        print_success(f"Python version {version_str} is compatible")
        return True

def check_node_version():
    """Check if Node.js is installed and version is compatible"""
    print_header("Checking Node.js Version")
    
    try:
        result = subprocess.run(["node", "--version"], capture_output=True, text=True, check=False)
        if result.returncode != 0:
            print_error("Node.js is not installed or not in PATH")
            return False
            
        version = result.stdout.strip()
        print_info(f"Detected Node.js version: {version}")
        
        # Extract major version number
        if version.startswith('v'):
            version = version[1:]
        major_version = int(version.split('.')[0])
        
        if major_version < 14:
            print_error(f"Node.js 14+ is required, but you have {version}")
            return False
        else:
            print_success(f"Node.js version {version} is compatible")
            return True
            
    except Exception as e:
        print_error(f"Failed to check Node.js version: {e}")
        return False

def check_required_packages():
    """Check if required Python packages are installed"""
    print_header("Checking Required Python Packages")
    
    package_mapping = {
        "fastapi": "fastapi",
        "uvicorn": "uvicorn",
        "websockets": "websockets",
        "pandas": "pandas",
        "networkx": "networkx",
        "langgraph": "langgraph",
        "pydantic": "pydantic",
        "openai": "openai",
        "pytz": "pytz",
        "python-multipart": "multipart",  # The actual module name is 'multipart'
        "requests": "requests",
        "matplotlib": "matplotlib",
        "numpy": "numpy"
    }
    
    missing_packages = []
    outdated_packages = {
        "openai": "1.77.0"  # Minimum required version
    }
    
    for package_name, module_name in package_mapping.items():
        try:
            module = importlib.import_module(module_name)
            if hasattr(module, "__version__"):
                version = module.__version__
                print_info(f"Found {package_name} version {version}")
                
                # Check if package has a minimum version requirement
                if package_name in outdated_packages and version < outdated_packages[package_name]:
                    print_warning(f"{package_name} version {version} is older than recommended version {outdated_packages[package_name]}")
            else:
                print_info(f"Found {package_name} (version unknown)")
        except ImportError:
            # Special case for python-multipart which doesn't have a clear module
            if package_name == "python-multipart":
                # Check using pip list
                try:
                    result = subprocess.run(["pip", "list"], capture_output=True, text=True, check=False)
                    if "python-multipart" in result.stdout:
                        print_info(f"Found {package_name} (version from pip)")
                        continue
                except Exception:
                    pass
            
            print_error(f"Package {package_name} is not installed")
            missing_packages.append(package_name)
    
    if missing_packages:
        print_error(f"Missing required packages: {', '.join(missing_packages)}")
        print_info("Install missing packages with: pip install -r requirements.txt")
        return False
    else:
        print_success("All required Python packages are installed")
        return True

def check_data_files():
    """Check if required data files exist"""
    print_header("Checking Data Files")
    
    data_dir = Path("01_Data")
    required_files = ["EFR.csv", "EQR.csv", "SKMS.csv"]
    
    if not data_dir.exists():
        print_error(f"Data directory '{data_dir}' does not exist")
        return False
        
    missing_files = []
    for file in required_files:
        file_path = data_dir / file
        if not file_path.exists():
            print_error(f"Data file '{file}' is missing")
            missing_files.append(file)
        else:
            print_success(f"Found data file: {file}")
    
    if missing_files:
        print_error(f"Missing data files: {', '.join(missing_files)}")
        return False
    else:
        print_success("All required data files are present")
        return True

def check_project_structure():
    """Check if the project structure is valid"""
    print_header("Checking Project Structure")
    
    required_dirs = ["backend", "ui"]
    required_files = ["start_fresh.sh", "README.md", "requirements.txt"]
    
    missing_items = []
    
    for directory in required_dirs:
        if not os.path.isdir(directory):
            print_error(f"Directory '{directory}' is missing")
            missing_items.append(directory)
        else:
            print_success(f"Found directory: {directory}")
    
    for file in required_files:
        if not os.path.isfile(file):
            print_error(f"File '{file}' is missing")
            missing_items.append(file)
        else:
            print_success(f"Found file: {file}")
    
    # Check if start_fresh.sh is executable
    if os.path.isfile("start_fresh.sh"):
        if not os.access("start_fresh.sh", os.X_OK):
            print_warning("start_fresh.sh is not executable. Run: chmod +x start_fresh.sh")
    
    if missing_items:
        print_error(f"Missing project items: {', '.join(missing_items)}")
        return False
    else:
        print_success("Project structure is valid")
        return True

def check_ports():
    """Check if required ports are available"""
    print_header("Checking Port Availability")
    
    required_ports = [9000, 3001]
    unavailable_ports = []
    
    for port in required_ports:
        try:
            # Try to bind to the port
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('127.0.0.1', port))
            if result == 0:
                print_warning(f"Port {port} is already in use")
                unavailable_ports.append(port)
            else:
                print_success(f"Port {port} is available")
            sock.close()
        except Exception as e:
            print_error(f"Failed to check port {port}: {e}")
    
    if unavailable_ports:
        print_warning(f"Some required ports are already in use: {', '.join(map(str, unavailable_ports))}")
        print_info("Make sure to stop any existing server processes before starting the application")
        return False
    else:
        print_success("All required ports are available")
        return True

def main():
    """Main function to run all checks"""
    print_header("NASDAQ GenAI Terminal - Setup Check")
    print_info(f"System: {platform.system()} {platform.release()}")
    print_info(f"Working directory: {os.getcwd()}")
    
    checks = [
        ("Python version", check_python_version),
        ("Node.js version", check_node_version),
        ("Required packages", check_required_packages),
        ("Data files", check_data_files),
        ("Project structure", check_project_structure),
        ("Port availability", check_ports)
    ]
    
    results = {}
    all_passed = True
    
    for name, check_func in checks:
        result = check_func()
        results[name] = result
        if not result:
            all_passed = False
    
    print_header("Setup Check Summary")
    for name, result in results.items():
        if result:
            print_success(f"{name}: Passed")
        else:
            print_error(f"{name}: Failed")
    
    if all_passed:
        print_success("\nAll checks passed! The system is ready to run.")
        print_info("Start the application with: ./start_fresh.sh")
    else:
        print_warning("\nSome checks failed. Please address the issues before running the application.")
    
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main()) 