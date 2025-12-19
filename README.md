![Python](https://img.shields.io/badge/python-3.x-blue)
![yfinance](https://img.shields.io/badge/yfinance-enabled-brightgreen)
![Multithreaded](https://img.shields.io/badge/concurrency-threaded-orange)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

---

## 🚀 Project Overview

This project is a **Python-based stock market data ingestion tool** that automatically downloads and incrementally updates **historical price data for Indian equities (NSE)** using **Yahoo Finance**.

It reads a list of stocks from a CSV file and maintains **per-company, per-timeframe CSV datasets**, intelligently handling Yahoo Finance API limitations for intraday data while supporting multithreaded execution for scalability.

---

## ⚙️ Key Features

* 📊 Fetches **historical OHLCV stock data** from Yahoo Finance
* ⏱️ Supports **multiple time intervals** (1m → 3mo)
* 🔁 **Incremental updates** (avoids re-downloading existing data)
* 🧵 **Multithreaded execution** with rate-limit safety
* 🗂️ Automatic **company-wise folder organization**
* 🧹 Data cleaning (duplicate timestamps & columns)
* 🇮🇳 Designed for **NSE stocks** (`.NS` suffix)

---

## 🤠 Tech Stack

* **Language:** Python 3.x
* **Core Libraries:**

  * `pandas`
  * `yfinance`
  * `tqdm`
  * `concurrent.futures`
* **Data Source:** Yahoo Finance API

---

## 🛠️ Installation & Setup

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/Arppittjaiin/YAHOO-STOCK-DATA.git
cd YAHOO-STOCK-DATA
```

### 2️⃣ Install Dependencies

```bash
pip install pandas yfinance tqdm
```

### 3️⃣ Prepare Input File

Ensure `EQUITY_L.csv` exists in the project root with at least the following columns:

```csv
SYMBOL,NAME OF COMPANY
RELIANCE,Reliance Industries Limited
TCS,Tata Consultancy Services
```

---

## 🚀 Usage

Run the script directly:

```bash
python fetch_stocks.py
```

### What Happens:

* Reads all stock symbols from `EQUITY_L.csv`
* Creates a folder per company
* Downloads or updates CSV files for each interval
* Displays a progress bar per stock

---

## 🛠️ How It Works

### 🔄 Supported Timeframes

| Interval | Max History (Yahoo Limit) |
| -------- | ------------------------- |
| 1m       | 7 days                    |
| 2m–30m   | 60 days                   |
| 60m / 1h | ~2 years                  |
| 1d+      | Full history              |

### 📂 Data Storage Logic

* **Daily & higher:** Uses `start=` for incremental updates
* **Intraday:** Rolling fetch (`period=`) + merge
* Handles:

  * Duplicate timestamps
  * Column duplication
  * Timezone mismatches

### 🧵 Concurrency

* Uses `ThreadPoolExecutor`
* Default: **4 worker threads**
* Randomized sleep to avoid rate-limit bursts

---

## 🛹 Folder Structure

```text
.
├── fetch_stocks.py
├── EQUITY_L.csv
├── Reliance Industries Limited/
│   ├── RELIANCE_1d.csv
│   ├── RELIANCE_1h.csv
│   └── RELIANCE_1m.csv
├── Tata Consultancy Services/
│   ├── TCS_1d.csv
│   └── TCS_15m.csv
```

---

## 📊 Configuration

### Editable Settings (in `fetch_stocks.py`)

```python
MAX_WORKERS = 4
SOURCE_FILE = 'EQUITY_L.csv'
```

### Interval Limits

Defined via `INTERVAL_LIMITS` to respect Yahoo Finance constraints.

---

## 📦 Dependencies

```txt
pandas>=1.5
yfinance>=0.2
tqdm>=4.64
```

---

## 👨‍💻 Author

**Arpit Jain (AJ)**
Senior Python Developer | Data Engineering & Market Analytics

---

## 📄 License

This project is licensed under the **MIT License**.

---
