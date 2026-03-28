# 🕷 Lolz Massive Crawler & Indexer (31M+ Records)

A high-performance, asynchronous data pipeline designed to scrape, process, and index millions of records from the Lolz forum into a **ClickHouse** analytical database. This system is built for speed, stability, and handling massive datasets.

## 🚀 Key Features:

* **Massive Scale:** Successfully indexed **31,000,000+** posts and comments.
* **Analytical Storage:** Integrated with **ClickHouse** using `ReplacingMergeTree` engines for lightning-fast queries and optimal data compression.
* **Smart Rate-Limiting:** Custom logic to handle `429 Too Many Requests` with dynamic delay adjustment and exponential backoff.
* **Infrastructure Monitoring:** Real-time throttling based on **CPU & RAM usage** to prevent server overloads during high-concurrency tasks.
* **Admin Dashboard:** Built-in Telegram interface to monitor crawling speed, database stats, and system health.
* **Data Integrity:** Checkpoint system to resume crawling from the last saved state, ensuring no data loss.

## 🛠 Tech Stack:

- **Core:** Python 3.10+ (Asyncio)
- **Database:** ClickHouse (High-performance OLAP)
- **Framework:** Aiogram 3.x (Admin Interface)
- **Networking:** Aiohttp (Async HTTP requests)
- **Monitoring:** Psutil (Resource tracking)

## 📊 Database Performance:
The system uses specialized ClickHouse engines to manage millions of rows while maintaining a small disk footprint thanks to advanced compression (ZSTD).

## 🚀 Installation & Setup:

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/sntroop/Lolz-Crawler.git](https://github.com/sntroop/Lolz-Crawler.git)
   cd Lolz-Crawler

 * Install dependencies:
   pip install -r requirements.txt

 * Configure Environment:
   Copy .env.example to .env and fill in your credentials (Bot Token, ClickHouse host, etc.).
 * Run the Engine:
   python main.py

⚠️ Disclaimer
This tool was developed for educational purposes and data analysis. Always respect the target platform's Terms of Service and robots.txt.

