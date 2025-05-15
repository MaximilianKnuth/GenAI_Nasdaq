# Changelog

## [1.0.0] - 2023-08-01

### Added
- Initial release of NASDAQ GenAI Terminal

## [1.1.0] - 2023-08-15

### Added
- Support for joining multiple datasets
- Improved error handling in the backend
- Better logging for debugging purposes

## [2.0.0] - 2024-05-01

### Added
- WebSocket heartbeat mechanism to keep connections alive
- Browser-based WebSocket testing interface (websocket_test.html)
- Example script for programmatic queries (example_query.py)
- Setup check script to verify installation (check_setup.py)
- Comprehensive usage guide (usage_guide.md)
- Detailed README with project structure and instructions

### Changed
- Updated OpenAI package to version 1.77.0
- Enhanced WebSocket connection stability
- Improved error handling for Safari browser compatibility
- Refined UI for better user experience

### Fixed
- Connection stability issues on Safari browsers
- Data loading errors for CSV files
- OpenAI client initialization errors
- Issues with repeated WebSocket connections opening and closing

### Security
- Added proper error handling to prevent potential security issues
- Improved validation of user inputs 