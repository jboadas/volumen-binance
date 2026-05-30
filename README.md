# Real-Time Binance Volume Scanner

A streamlined, high-performance Python tool designed to monitor and scan live cryptocurrency market volume and order book dynamics via the Binance WebSocket API.

## Project Structure

* **`main.py`:** The main orchestrator and entry point. It initializes the execution context, manages the core asynchronous runtime, and coordinates the scanning process.
* **`scanner.py`:** The heavy-lifter of the project. It handles the live stream connection to Binance, captures the high-frequency market events, and executes the scanning and filtering logic to detect volume patterns or market imbalances in real-time.

## Key Technical Features
* **Asynchronous Execution:** Built on top of Python's async architecture to process high-frequency financial data streams without blocking execution.
* **Low-Overhead Architecture:** Zero bloat. The entire scanning pipeline is optimized into direct, cohesive modules (`main.py` and `scanner.py`) to reduce latency and execution overhead.
* **Live Streaming:** Connects directly to Binance WebSockets to scan market volume tick-by-tick as trades occur.

## Tech Stack
* **Language:** Python (Asyncio)
* **Data Source:** Binance WebSocket API