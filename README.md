# 🚀 Fraud Detection Streaming Platform

Real-time fraud detection system built with a modern data stack: **Kafka + Spark Structured Streaming + PostgreSQL + Streamlit**.

---

## 📸 Overview

This project simulates a **real-world banking fraud detection system**, capable of ingesting, processing, and detecting anomalies in streaming transactions.

---

## 🧩 Architecture

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
