# Real-Time Market Volume & Imbalance Processor

A high-performance Python application designed to ingest real-time cryptocurrency streams from the Binance WebSocket API, calculating volume dynamics and order book imbalances instantly in-memory using Redis.

## Architectural Focus: Low-Latency In-Memory Analytics

* **Sub-Millisecond Processing:** Designed specifically for financial data scenarios where traditional relational database writes introduce unacceptable latency disk bottlenecks.
* **State Management via Redis:** Leverages Redis data structures to track tick-by-tick volume accumulation, buyer/seller strength, and market metrics concurrently without losing state.
* **Stream Synchronization:** Features a structured control loop (`control.py`) to handle high-frequency execution blocks, ensuring real-time metrics are always accurate and available for algorithmic execution.

## Tech Stack
* **Language:** Python (Asyncio / Advanced Stream Handling)
* **In-Memory Store & Analytics:** Redis
* **Data Source:** Binance WebSocket API (Live AggTrade / Order Book Streams)