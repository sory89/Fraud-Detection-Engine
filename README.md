# 🚀 Fraud Detection Streaming Platform

Real-time fraud detection system built with a modern data stack: **Kafka + Spark Structured Streaming + PostgreSQL + Streamlit**.

---

## 📸 Overview

This project simulates a **real-world banking fraud detection system**, capable of ingesting, processing, and detecting anomalies in streaming transactions.

---

## 🏗️ Architecture


---

## 🎯 Version plus détaillée (niveau pro 🔥)

```markdown
## 🧠 Detailed Architecture

```mermaid
flowchart LR

    subgraph Ingestion
        A[Transaction Generator]
        B[Kafka Topic: transactions]
        A --> B
    end

    subgraph Processing
        C[Spark Streaming]
        D[Fraud Detection Engine]
        B --> C
        C --> D
    end

    subgraph Storage
        E[PostgreSQL]
        C --> E
    end

    subgraph Outputs
        F[Alerts]
        G[Monitoring Metrics]
        H[Streamlit Dashboard]
    end

    D --> F
    D --> G

    E --> H
    F --> H
    G --> H
