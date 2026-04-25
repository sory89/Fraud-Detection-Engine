# 🚀 Fraud Detection Streaming Platform

Real-time fraud detection system built with a modern data stack: **Kafka + Spark Structured Streaming + PostgreSQL + Streamlit**.

---

## 📸 Overview

This project simulates a **real-world banking fraud detection system**, capable of ingesting, processing, and detecting anomalies in streaming transactions.

---

## 🏗️ Architecture

```mermaid
flowchart LR

    A[Transaction Generator] --> B[Kafka]

    B --> C[Spark Streaming]

    C --> D[PostgreSQL]

    C --> E[Fraud Detection Engine]

    E --> F[Alerts System]

    E --> G[Monitoring]

    D --> H[Streamlit Dashboard]

    F --> H
    G --> H


---

## ⚙️ Tech Stack

- 🐍 Python  
- ⚡ Apache Kafka  
- 🔥 Apache Spark Structured Streaming  
- 🐘 PostgreSQL  
- 📊 Streamlit  
- 🐳 Docker Compose  

---

## 🎯 Features

### 📡 Real-time ingestion
- Simulated transaction generator
- Kafka topics:
  - `transactions`
  - `fraud_alerts`

### ⚡ Streaming processing
- Spark Structured Streaming pipeline
- Window aggregations (1 min / 5 min)

### 🚨 Fraud detection rules
- High amount transactions  
- Suspicious IP / location  
- High-risk country  
- Burst transactions (rate anomaly)  

### 🧠 Alert system
- Real-time fraud alerts  
- Dead Letter Queue (DLQ)  
- Acknowledgement tracking  

### 📊 Dashboard (Streamlit)
- Transactions vs fraud rate  
- Fraud alerts monitoring  
- Fraud by rule distribution  
- Pipeline health  

---

## 📊 Dashboard Highlights

- ✅ Live transaction monitoring  
- 🚨 Fraud alerts (acknowledged / unacknowledged)  
- 📈 Fraud rate analytics  
- 🔍 Rule-based breakdown  

---

## 🗄️ Data Model

### `transactions`

| column          | type      |
|-----------------|----------|
| transaction_id  | string   |
| user_id         | string   |
| amount          | float    |
| location        | string   |
| timestamp       | timestamp|

---

### `fraud_alerts`

| column          | type      |
|-----------------|----------|
| alert_id        | string   |
| transaction_id  | string   |
| rule            | string   |
| status          | string   |
| created_at      | timestamp|

---

## 🐳 Run Locally

### 1. Clone repository

```bash
git clone https://github.com/your-repo/fraud-detection-platform.git
cd fraud-detection-platform
docker compose up -d
