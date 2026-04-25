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

    E --> F[Alerts]

    E --> G[Monitoring]

    D --> H[Streamlit Dashboard]

    F --> H
    G --> H

git clone https://github.com/your-repo/fraud-detection-platform.git
cd fraud-detection-platform
docker compose up -d

<img width="958" height="475" alt="image" src="https://github.com/user-attachments/assets/91de30bc-1bb9-47a4-b7d4-b0ca962c3f08" />
